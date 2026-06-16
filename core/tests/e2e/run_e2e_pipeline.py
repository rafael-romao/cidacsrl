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
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CIDACS-RL-E2E")

ES_URL = os.environ.get("CIDACSRL_ES_URL", "http://elasticsearch:9200")
linkage_spec_file = "linkage_acidentes_obitos.yml"
index_spec_file = "obitos_example_index.yml"


def _tests_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _configs_root() -> Path:
    return _tests_root() / "configs"


def _shared_configs_root() -> Path:
    return _configs_root() / "shared"


def _build_runtime_env_config() -> Path:
    parsed_url = urlparse(ES_URL)
    es_host = parsed_url.hostname or "localhost"
    es_port = parsed_url.port or 9200

    runtime_env = {
            "storage": {
                "source_path": str(_tests_root() / "data" / "input"),
                "source_format": "parquet",
                "output_path": str(_tests_root() / "data" / "output"),
                "output_format": "parquet"
            },
            "execution": {
                "audit_log_path": str(_tests_root() / "data" / "output" / "_audit"),
                "partitioning": {
                    "partition_column": "uf_internacao",
                    "filter_partitions": ["BA", "SP", "BU"]
                }
            },
            "specification": {
                "indexing_path": str(_shared_configs_root() / index_spec_file),
                "linkage_path": str(_shared_configs_root() / linkage_spec_file)
            },
            "spark": {
                "spark_configs": {
                    "spark.master": "local[*]",
                    "spark.sql.shuffle.partitions": "2",
                    "spark.ui.enabled": "false",
                    "spark.jars.packages": "org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8",
                    "spark.port.maxRetries": "100"
                }
            },
            "elasticsearch": {
                "host": es_host,
                "port": es_port,
                "es_connection_url": ES_URL,
                "wan_only": True,
                "search_strategy": "multisearch" 
            }
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
    logger.info("Passo 2/2: Disparando motor de Record Linkage das fontes Parquet...")
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

    index_spec = _read_index_spec(index_spec_path)
    index_name = index_spec["index_config"]["name"]

    # 1. Validação no Elasticsearch
    es = Elasticsearch(ES_URL)
    es.indices.refresh(index=index_name)
    res = es.search(index=index_name, query={"match_all": {}})
    total_docs = res["hits"]["total"]["value"]
    logger.info(f"Auditoria ES: Encontrados {total_docs} documentos no índice '{index_name}'.")
    assert total_docs > 0, f"Índice '{index_name}' está vazio após a execução."

    # 2. Validação no Disco (Leitura adaptada à nova árvore profunda por projeto/job/unit/phase)
    with open(linkage_spec_path) as f:
        linkage_spec = yaml.safe_load(f)

    source_table = linkage_spec["source_table"]
    target_es_index = linkage_spec["target_es_index"]
    
    # Determina deterministicamente o nome do projeto de acordo com a regra de negócio do domínio
    project_dir_name = f"linkage_{source_table}_{target_es_index}"
    project_output_path = base_output_path / project_dir_name

    logger.info(f"Varrendo os resultados consolidados na árvore do projeto: {project_output_path}")
    assert project_output_path.exists(), f"Erro Crítico: Pasta do projeto de linkage não foi criada: {project_output_path}"

    # Localiza dinamicamente as pastas de jobs geradas dentro do projeto
    job_dirs = [p for p in project_output_path.iterdir() if p.is_dir() and p.name.startswith("job_")]
    assert job_dirs, f"Erro Crítico: Nenhuma pasta de execução de Job válida encontrada em {project_output_path}"
    
    # Seleciona o diretório do Job mais recente gerado pelo teste corrente
    active_job_path = max(job_dirs, key=lambda p: p.stat().st_mtime)
    logger.info(f"Execução ativa detectada para auditoria de dados: {active_job_path.name}")

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("E2E-Output-Validation")
        .getOrCreate()
    )

    try:
        all_phases_df = spark.read.parquet(f"{active_job_path}/*/*")
        
        total_links = all_phases_df.count()
        rows = all_phases_df.limit(5).collect()
        
        logger.info(f"Auditoria Disco: Total de pares linkados gerados pelo motor: {total_links}")
        assert total_links > 0, "O pipeline rodou, mas nenhum par de matching foi gerado após a filtragem."

        logger.info("Amostra dos Pares Identificados e Armazenados:")
        for i, row in enumerate(rows):
            logger.info(
                f"  [Par #{i + 1}] Origem ID: {row.source_codigo_internacao} "
                f"-> Candidato ES ID: {row.candidate_codigo_nascimento} "
                f"| Score: {row.match_score} | Fase: {row.phase_match}"
            )
            
    finally:
        spark.stop()


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

        index_spec_path = _shared_configs_root() / index_spec_file
        linkage_spec_path = _shared_configs_root() / linkage_spec_file

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