import argparse
import os
import subprocess
import sys
import tempfile
import yaml
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CIDACS-RL-E2E")

ES_URL = os.environ.get("CIDACSRL_ES_URL", "http://elasticsearch:9200")


def _tests_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _configs_root() -> Path:
    return _tests_root() / "configs"


def _shared_configs_root() -> Path:
    return _configs_root() / "shared"


def _build_runtime_env_config() -> Path:
    runtime_env = {
        "source_data_path": str(_tests_root() / "data" / "input"),
        "output_data_path": str(_tests_root() / "data" / "output"),
        "source_data_format": "parquet",
        "output_data_format": "parquet",
        "es_config_path": str(_configs_root() / "integration" / "es_local.yml"),
        "spark_config_path": str(_shared_configs_root() / "spark_local.yml"),
    }
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False)
    with temp_file:
        yaml.safe_dump(runtime_env, temp_file, sort_keys=False)
    return Path(temp_file.name)


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


def _read_index_spec(spec_path: Path) -> dict:
    with open(spec_path) as f:
        return yaml.safe_load(f)


def run_indexing_step(runtime_env_path: Path) -> None:
    logger.info("Passo 1/2: Disparando processo de Indexação em Massa (Bulk) no ES...")
    result = subprocess.run(
        [
            "python", "/app/cidacsrl_rlp/cli.py", "indexing",
            "--env-config", str(runtime_env_path),
            "--spec-config", str(_shared_configs_root() / "index_spec_local.yml"),
        ],
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"O Adaptador CLI reportou erro na execução do Caso de Uso de Indexação (exit {result.returncode})."
        )


def run_linkage_step(runtime_env_path: Path) -> None:
    logger.info("Passo 2/2: Disparando motor de Record Linkage das fontes Parquet...")
    result = subprocess.run(
        [
            "python", "/app/cidacsrl_rlp/cli.py", "linkage",
            "--env-config", str(runtime_env_path),
            "--spec-config", str(_shared_configs_root() / "linkage_spec_local.yml"),
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

    index_spec = _read_index_spec(index_spec_path)
    index_name = index_spec["index_config"]["name"]

    # 1. Validação no Elasticsearch
    es = Elasticsearch(ES_URL)
    es.indices.refresh(index=index_name)
    res = es.search(index=index_name, query={"match_all": {}})
    total_docs = res["hits"]["total"]["value"]
    logger.info(f"Auditoria ES: Encontrados {total_docs} documentos no índice '{index_name}'.")
    assert total_docs > 0, f"Índice '{index_name}' está vazio após a execução."

    # 2. Validação no Disco (Camada de Linkage)
    with open(linkage_spec_path) as f:
        linkage_spec = yaml.safe_load(f)

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("E2E-Output-Validation")
        .getOrCreate()
    )

    all_phases_df = None
    for i, phase in enumerate(linkage_spec.get("blocking_phases", [])):
        if phase.get("enabled", False):
            phase_name = phase["phase_name"]
            phase_output_path = base_output_path / f"phase_{i + 1}_{phase_name}"
            logger.info(f"Lendo resultados da fase: {phase_output_path}")
            assert phase_output_path.exists(), (
                f"Erro Crítico: pasta de resultados não gerada em: {phase_output_path}"
            )
            df_phase = spark.read.parquet(str(phase_output_path))
            all_phases_df = df_phase if all_phases_df is None else all_phases_df.unionByName(df_phase)

    assert all_phases_df is not None, "Nenhuma fase de linkage habilitada foi encontrada ou processada."

    total_links = all_phases_df.count()
    rows = all_phases_df.limit(5).collect()
    spark.stop()

    logger.info(f"Auditoria Disco: Total de pares linkados gerados pelo motor: {total_links}")
    assert total_links > 0, "O pipeline rodou, mas nenhum par de matching foi gerado."

    logger.info("Amostra dos Pares Identificados:")
    for i, row in enumerate(rows):
        logger.info(
            f"  [Par #{i + 1}] Origem ID: {row.source_codigo_internacao} "
            f"-> Candidato ES ID: {row.candidate_codigo_nascimento} | Score: {row.match_score}"
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CIDACS-RL E2E Pipeline Runner")
    parser.add_argument(
        "--skip-indexing",
        action="store_true",
        help="Pula o passo de indexação. Útil quando o índice já está populado.",
    )
    parser.add_argument(
        "--skip-linkage",
        action="store_true",
        help="Pula o passo de linkage. Útil para validar apenas a indexação.",
    )
    parser.add_argument(
        "--auto-skip-indexing",
        action="store_true",
        help=(
            "Verifica automaticamente se o índice já está populado e pula a "
            "indexação se estiver. Comportamento idempotente."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    runtime_env_path = _build_runtime_env_config()
    try:
        logger.info("=========================================================================")
        logger.info("               INICIANDO PIPELINE DE INTEGRAÇÃO FIM-A-FIM                ")
        logger.info("=========================================================================")

        index_spec_path = _shared_configs_root() / "index_spec_local.yml"
        linkage_spec_path = _shared_configs_root() / "linkage_spec_local.yml"

        # --- Passo 1: Indexação ---
        skip_indexing = args.skip_indexing
        if args.auto_skip_indexing and not skip_indexing:
            index_spec = _read_index_spec(index_spec_path)
            index_name = index_spec["index_config"]["name"]
            skip_indexing = _index_already_populated(index_name, expected_docs=100)

        if skip_indexing:
            logger.info("Passo 1/2: Indexação ignorada (--skip-indexing ou índice já populado).")
        else:
            run_indexing_step(runtime_env_path)

        # --- Passo 2: Linkage ---
        if args.skip_linkage:
            logger.info("Passo 2/2: Linkage ignorado (--skip-linkage).")
        else:
            run_linkage_step(runtime_env_path)

        # --- Passo 3: Validação ---
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
        logger.error(f"❌ Falha no Pipeline de Integração E2E: {e}")
        sys.exit(1)
    finally:
        try:
            runtime_env_path.unlink(missing_ok=True)
        except Exception:
            pass