import argparse
import os
import subprocess
import sys
import yaml
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("E2E Pipeline Runner")

ES_URL = os.environ.get("CIDACSRL_ES_URL", "http://elasticsearch:9200")


def _tests_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _configs_root() -> Path:
    return _tests_root() / "configs"


def _read_yaml(file_path: Path) -> dict:
    with open(file_path) as f:
        return yaml.safe_load(f)


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
    res = es.search(index=index_name, query={"match_all": {}})
    total_docs = res["hits"]["total"]["value"]
    logger.info(f"Auditoria ES: Encontrados {total_docs} documentos no índice '{index_name}'.")
    assert total_docs > 0, f"Índice '{index_name}' está vazio após a execução."

    # 2. Validação no Disco
    linkage_spec = _read_yaml(linkage_spec_path)

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
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    
    # 1. Resolve o path do Env File passado pelo usuário/Makefile
    env_filename = args.env_name if args.env_name.endswith((".yml", ".yaml")) else f"{args.env_name}.yml"
    runtime_env_path = _configs_root() / env_filename
    
    if not runtime_env_path.exists():
        logger.error(f"Arquivo de ambiente não encontrado: {runtime_env_path}")
        sys.exit(1)

    try:
        logger.info("=========================================================================")
        logger.info(f"       INICIANDO PIPELINE DE INTEGRAÇÃO FIM-A-FIM: {env_filename}       ")
        logger.info("=========================================================================")

        # 2. Extrai os paths de especificação de dentro do arquivo de env selecionado
        env_config = _read_yaml(runtime_env_path)
        project_root = _tests_root().parent  # Assume que os caminhos no yaml são relativos à raiz do projeto
        
        
        
        # Resolve os caminhos
        idx_path_str = env_config["specification"]["indexing_path"].lstrip("/")
        lnk_path_str = env_config["specification"]["linkage_path"].lstrip("/")

        index_spec_path = project_root / idx_path_str
        linkage_spec_path = project_root / lnk_path_str

        print(f"Ambiente selecionado: {env_filename}"
              f"\n- Especificação de Indexação: {index_spec_path}"
              f"\n- Especificação de Linkage: {linkage_spec_path}\n")

        # --- Passo 1: Indexação  ---
        index_spec = _read_yaml(index_spec_path)
        index_name = index_spec["index_config"]["name"]
        
        # Assume que o índice já está populado se tiver mais de 100 documentos
        skip_indexing = _index_already_populated(index_name, expected_docs=100)

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
        if not args.skip_linkage:
            verify_and_validate_e2e_results(
                index_spec_path=index_spec_path,
                linkage_spec_path=linkage_spec_path,
                base_output_path=_tests_root() / "data" / "output",
            )
        else:
            logger.info("Atenção: Validação dos resultados em disco foi ignorada pois o Linkage não foi executado.")

        logger.info("=========================================================================")
        logger.info("     STATUS FINAL: SUCESSO")
        logger.info("=========================================================================")
        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Falha no Pipeline de Integração E2E: {e}")
        sys.exit(1)