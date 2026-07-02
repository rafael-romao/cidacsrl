import argparse
import os
import subprocess
import sys
import yaml
import logging
from pathlib import Path
from elasticsearch import Elasticsearch
import pyarrow.dataset as ds

from cidacsrl.config.logging import configure_logging

configure_logging()
logger = logging.getLogger("E2E Pipeline Runner")

ES_URL = os.environ.get("CIDACSRL_ES_URL", "http://elasticsearch:9200")
SUBPROCESS_TIMEOUT = int(os.environ.get("CIDACSRL_E2E_TIMEOUT", "3600"))


def _tests_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _configs_root() -> Path:
    return _tests_root() / "fixtures" / "configs" / "env"


def _read_yaml(file_path: Path) -> dict:
    with open(file_path) as f:
        return yaml.safe_load(f)


def _count_source_records(source_path: Path, source_table: str) -> int:
    table_path = source_path / source_table
    if not table_path.exists():
        logger.warning(f"Caminho de origem não encontrado: {table_path}. Usando threshold mínimo.")
        return 0
    return ds.dataset(str(table_path), format="parquet").count_rows()


def _index_already_populated(es: Elasticsearch, index_name: str, expected_docs: int) -> bool:
    """Retorna True se o índice existe e já contém ao menos expected_docs documentos."""
    try:
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


def _run_cli_step(step_label: str, use_case: str, runtime_env_path: Path, project_root: Path) -> None:
    logger.info(f"{step_label}: usando {runtime_env_path.name}...")
    result = subprocess.run(
        [
            "poetry", "run", "python", "-m", "cidacsrl.adapters.inbound.cli", use_case,
            "--env-config", str(runtime_env_path),
        ],
        cwd=project_root,
        check=False,
        timeout=SUBPROCESS_TIMEOUT,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"O Adaptador CLI reportou erro na execução do Caso de Uso de {use_case} (exit {result.returncode})."
        )


def verify_and_validate_e2e_results(
    es: Elasticsearch,
    index_spec: dict,
    linkage_spec: dict,
    base_output_path: Path,
) -> None:
    logger.info("Iniciando varredura e validação pós-execução nos volumes compartilhados...")

    index_name = index_spec["index_config"]["name"]
    es.indices.refresh(index=index_name)
    total_docs = es.count(index=index_name)["count"]
    logger.info(f"Auditoria ES: Encontrados {total_docs} documentos no índice '{index_name}'.")
    if total_docs == 0:
        raise RuntimeError(f"Índice '{index_name}' está vazio após a execução.")

    source_table = linkage_spec["source_table"]
    target_es_index = linkage_spec["target_es_index"]
    project_output_path = base_output_path / f"linkage_{source_table}_{target_es_index}"

    logger.info(f"Varrendo os resultados consolidados na árvore do projeto: {project_output_path}")
    if not project_output_path.exists():
        raise RuntimeError(f"Pasta do projeto de linkage não foi criada: {project_output_path}")

    phase_dirs = [p for p in project_output_path.iterdir() if p.is_dir() and not p.name.startswith(".")]
    if not phase_dirs:
        raise RuntimeError(f"Nenhuma pasta de fase válida encontrada em {project_output_path}")

    expected_phases = {
        p["phase_name"] for p in linkage_spec.get("blocking_phases", []) if p.get("enabled", True)
    }
    found_phases = {
        p.name.split("=", 1)[1] if "=" in p.name else p.name
        for p in phase_dirs
    }
    missing_phases = expected_phases - found_phases
    if missing_phases:
        raise RuntimeError(f"Fases habilitadas sem output em disco: {missing_phases}")

    logger.info(f"Fases detectadas para auditoria: {sorted(found_phases)}")

    dataset = ds.dataset(str(project_output_path), format="parquet")
    total_links = dataset.count_rows()
    logger.info(f"Auditoria Disco: Total de pares linkados gerados pelo motor: {total_links}")
    if total_links == 0:
        raise RuntimeError("O pipeline rodou, mas nenhum par de matching foi gerado após a filtragem.")

    logger.info("Amostra dos Pares Identificados e Armazenados:")
    for i, row_dict in enumerate(dataset.head(5).to_pylist()):
        source_keys = [k for k in row_dict if k.startswith("source_")]
        candidate_keys = [k for k in row_dict if k.startswith("candidate_")]
        src_col = source_keys[0] if source_keys else "source_id"
        cand_col = candidate_keys[0] if candidate_keys else "candidate_id"
        logger.info(
            f"  [Par #{i + 1}] Origem ({src_col}): {row_dict.get(src_col, 'N/A')} "
            f"-> Candidato ES ({cand_col}): {row_dict.get(cand_col, 'N/A')} "
            f"| Score: {row_dict.get('match_score', 'N/A')} "
            f"| Fase: {row_dict.get('phase_match', 'N/A')}"
        )


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
    args = parser.parse_args()
    if args.skip_linkage and args.validate_only:
        parser.error("--skip-linkage e --validate-only são mutuamente exclusivos.")
    return args


def main(args: argparse.Namespace) -> None:
    env_filename = args.env_name if args.env_name.endswith((".yml", ".yaml")) else f"{args.env_name}.yml"
    runtime_env_path = _configs_root() / env_filename

    if not runtime_env_path.exists():
        logger.error(f"Arquivo de ambiente não encontrado: {runtime_env_path}")
        sys.exit(1)

    logger.info("=========================================================================")
    logger.info(f"       INICIANDO PIPELINE DE INTEGRAÇÃO FIM-A-FIM: {env_filename}       ")
    logger.info("=========================================================================")

    env_config = _read_yaml(runtime_env_path)
    project_root = _tests_root().parent

    index_spec_path = project_root / env_config["specification"]["indexing_path"].lstrip("/")
    linkage_spec_path = project_root / env_config["specification"]["linkage_path"].lstrip("/")

    logger.info(
        f"Ambiente selecionado: {env_filename}"
        f"\n- Especificação de Indexação: {index_spec_path}"
        f"\n- Especificação de Linkage: {linkage_spec_path}"
    )

    index_spec = _read_yaml(index_spec_path)
    linkage_spec = _read_yaml(linkage_spec_path)
    es = Elasticsearch(ES_URL)
    output_path = project_root / env_config["storage"]["output_path"].lstrip("/")

    if args.validate_only:
        logger.info("Modo --validate-only: indexação e linkage serão ignorados.")
    else:
        index_name = index_spec["index_config"]["name"]
        source_table = index_spec["source_config"]["source_table"]
        source_path = project_root / env_config["storage"]["source_path"].lstrip("/")
        expected_docs = _count_source_records(source_path, source_table)

        if expected_docs > 0 and _index_already_populated(es, index_name, expected_docs=expected_docs):
            logger.info("Passo 1/2: Indexação ignorada (o índice já está devidamente populado).")
        else:
            _run_cli_step("Passo 1/2: Indexação em Massa (Bulk) no ES", "indexing", runtime_env_path, project_root)

        if args.skip_linkage:
            logger.info("Passo 2/2: Linkage ignorado via flag (--skip-linkage).")
        else:
            _run_cli_step("Passo 2/2: Record Linkage", "linkage", runtime_env_path, project_root)

    if not args.skip_linkage:
        verify_and_validate_e2e_results(
            es=es,
            index_spec=index_spec,
            linkage_spec=linkage_spec,
            base_output_path=output_path,
        )

    logger.info("=========================================================================")
    logger.info("     STATUS FINAL: SUCESSO")
    logger.info("=========================================================================")


if __name__ == "__main__":
    try:
        main(_parse_args())
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Falha no pipeline: {e}")
        sys.exit(1)
