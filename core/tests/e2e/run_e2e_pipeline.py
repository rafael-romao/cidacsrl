import argparse
import os
import subprocess
import sys
import yaml
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
import pyarrow.dataset as ds

from cidacsrl.config.logging import configure_logging

configure_logging()
logger = logging.getLogger("E2E Pipeline Runner")

ES_URL = os.environ.get("CIDACSRL_ES_URL", "http://elasticsearch:9200")


def _tests_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _configs_root() -> Path:
    return _tests_root() / "configs"


def _read_yaml(file_path: Path) -> dict:
    with open(file_path) as f:
        return yaml.safe_load(f)


def _count_source_records(source_path: Path, source_table: str) -> int:
    table_path = source_path / source_table
    if not table_path.exists():
        logger.warning(f"Caminho de origem não encontrado: {table_path}. Usando threshold mínimo.")
        return 0
    dataset = ds.dataset(str(table_path), format="parquet")
    return dataset.count_rows()


def _index_already_populated(index_name: str, expected_docs: int) -> bool:
    """Retorna True se o índice existe e já contém ao menos expected_docs documentos."""
    try:
        es = Elasticsearch(ES_URL)
        if not es.indices.exists(index=index_name):
            return False
        count = es.count(index=index_name)["count"]
        already = count >= expected_docs
        if already:
            logger.info(
                f"Índice '{index_name}' já contém {count} documentos "
                f"(esperado >= {expected_docs}). Indexação será ignorada."
            )
        return already
    except Exception as exc:
        logger.warning(f"Não foi possível verificar o índice '{index_name}': {exc}")
        return False


def run_indexing_step(runtime_env_path: Path) -> None:
    logger.info(f"Passo 1/2: Disparando processo de Indexação em Massa (Bulk) no ES usando {runtime_env_path.name}...")
    result = subprocess.run(
        [
            "poetry", "run", "python", "-m", "cli", "indexing",
            "--env-config", str(runtime_env_path)
        ],
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"O Adaptador CLI reportou erro na execução do Caso de Uso de Indexação (exit {result.returncode})."
        )


def run_linkage_step(runtime_env_path: Path) -> None:
    logger.info(f"Passo 2/2: Disparando motor de Record Linkage usando {runtime_env_path.name}...")
    result = subprocess.run(
        [
            "poetry", "run", "python", "-m", "cli", "linkage",
            "--env-config", str(runtime_env_path)
        ],
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"O Adaptador CLI reportou erro na execução do Caso de Uso de Linkage (exit {result.returncode})."
        )


def verify_and_validate_e2e_results(
    index_spec_path: Path,
    linkage_spec_path: Path,
    base_output_path: Path,
) -> None:
    logger.info("Iniciando varredura e validação pós-execução nos volumes compartilhados...")

    index_spec = _read_yaml(index_spec_path)
    index_name = index_spec["index_config"]["name"]

    # 1. Validação no Elasticsearch
    es = Elasticsearch(ES_URL)
    es.indices.refresh(index=index_name)
    total_docs = es.count(index=index_name)["count"]
    logger.info(f"Auditoria ES: Encontrados {total_docs} documentos no índice '{index_name}'.")
    assert total_docs > 0, f"Índice '{index_name}' está vazio após a execução."

    # 2. Validação no Disco
    linkage_spec = _read_yaml(linkage_spec_path)

    source_table = linkage_spec["source_table"]
    target_es_index = linkage_spec["target_es_index"]

    project_dir_name = f"linkage_{source_table}_{target_es_index}"
    project_output_path = base_output_path / project_dir_name

    logger.info(f"Varrendo os resultados consolidados na árvore do projeto: {project_output_path}")
    assert project_output_path.exists(), f"Erro Crítico: Pasta do projeto de linkage não foi criada: {project_output_path}"

    # Localiza as pastas de fases geradas (estrutura Hive: project/phase_match=phase_name/...)
    phase_dirs = [p for p in project_output_path.iterdir() if p.is_dir() and not p.name.startswith(".")]
    assert phase_dirs, f"Erro Crítico: Nenhuma pasta de fase válida encontrada em {project_output_path}"

    # Cross-check: todas as fases habilitadas no spec devem ter produzido output
    expected_phases = {
        p["phase_name"] for p in linkage_spec.get("blocking_phases", []) if p.get("enabled", True)
    }
    found_phases = {
        p.name.split("=", 1)[1] if "=" in p.name else p.name
        for p in phase_dirs
    }
    missing_phases = expected_phases - found_phases
    assert not missing_phases, f"Erro Crítico: Fases habilitadas sem output em disco: {missing_phases}"

    logger.info(f"Fases detectadas para auditoria: {sorted(found_phases)}")

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("E2E-Output-Validation")
        .getOrCreate()
    )

    try:
        all_phases_df = spark.read.option("mergeSchema", "true").parquet(str(project_output_path))

        total_links = all_phases_df.count()
        rows = all_phases_df.limit(5).collect()

        logger.info(f"Auditoria Disco: Total de pares linkados gerados pelo motor: {total_links}")
        assert total_links > 0, "O pipeline rodou, mas nenhum par de matching foi gerado após a filtragem."

        logger.info("Amostra dos Pares Identificados e Armazenados:")
        for i, row in enumerate(rows):
            row_dict = row.asDict()

            # Extração agnóstica das chaves independentemente do domínio (Nascimento, Acidente, etc)
            source_keys = [k for k in row_dict.keys() if k.startswith("source_") and not k.startswith("source_uf")]
            candidate_keys = [k for k in row_dict.keys() if k.startswith("candidate_")]

            src_col = source_keys[0] if source_keys else "source_id"
            cand_col = candidate_keys[0] if candidate_keys else "candidate_id"

            logger.info(
                f"  [Par #{i + 1}] Origem ({src_col}): {row_dict.get(src_col, 'N/A')} "
                f"-> Candidato ES ({cand_col}): {row_dict.get(cand_col, 'N/A')} "
                f"| Score: {row_dict.get('match_score', 'N/A')} "
                f"| Fase: {row_dict.get('phase_match', 'N/A')}"
            )

    finally:
        spark.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CIDACS-RL E2E Pipeline Runner Agnóstico")
    parser.add_argument(
        "--env-name",
        type=str,
        required=True,
        help="Nome do arquivo YAML de ambiente localizado na pasta configs (ex: linkage_acidentes_obitos_env.yml)",
    )
    parser.add_argument(
        "--skip-linkage",
        action="store_true",
        help="Pula o passo de linkage e sua validação de disco. Útil para executar apenas a indexação.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Pula indexação e linkage; executa apenas a validação dos resultados já existentes em disco e ES.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    env_filename = args.env_name if args.env_name.endswith((".yml", ".yaml")) else f"{args.env_name}.yml"
    runtime_env_path = _configs_root() / env_filename

    if not runtime_env_path.exists():
        logger.error(f"Arquivo de ambiente não encontrado: {runtime_env_path}")
        sys.exit(1)

    try:
        logger.info("=========================================================================")
        logger.info(f"       INICIANDO PIPELINE DE INTEGRAÇÃO FIM-A-FIM: {env_filename}       ")
        logger.info("=========================================================================")

        env_config = _read_yaml(runtime_env_path)
        project_root = _tests_root().parent

        idx_path_str = env_config["specification"]["indexing_path"].lstrip("/")
        lnk_path_str = env_config["specification"]["linkage_path"].lstrip("/")

        index_spec_path = project_root / idx_path_str
        linkage_spec_path = project_root / lnk_path_str

        logger.info(
            f"Ambiente selecionado: {env_filename}"
            f"\n- Especificação de Indexação: {index_spec_path}"
            f"\n- Especificação de Linkage: {linkage_spec_path}"
        )

        if args.validate_only:
            logger.info("Modo --validate-only: indexação e linkage serão ignorados.")
        else:
            # --- Passo 1: Indexação ---
            index_spec = _read_yaml(index_spec_path)
            index_name = index_spec["index_config"]["name"]
            source_table = index_spec["source_config"]["source_table"]
            source_path = project_root / env_config["storage"]["source_path"].lstrip("/")
            expected_docs = _count_source_records(source_path, source_table)

            skip_indexing = expected_docs > 0 and _index_already_populated(index_name, expected_docs=expected_docs)

            if skip_indexing:
                logger.info("Passo 1/2: Indexação ignorada (o índice já está devidamente populado).")
            else:
                run_indexing_step(runtime_env_path)

            # --- Passo 2: Linkage ---
            if args.skip_linkage:
                logger.info("Passo 2/2: Linkage ignorado via flag (--skip-linkage).")
            else:
                run_linkage_step(runtime_env_path)

        # --- Passo 3: Validação ---
        if args.skip_linkage and not args.validate_only:
            logger.info("Atenção: Validação dos resultados em disco foi ignorada pois o Linkage não foi executado.")
        else:
            verify_and_validate_e2e_results(
                index_spec_path=index_spec_path,
                linkage_spec_path=linkage_spec_path,
                base_output_path=_tests_root() / "data" / "output",
            )

        logger.info("=========================================================================")
        logger.info("     STATUS FINAL: SUCESSO")
        logger.info("=========================================================================")
        sys.exit(0)

    except Exception as e:
        logger.exception(f"❌ Falha no pipeline: {e}")
        sys.exit(1)
