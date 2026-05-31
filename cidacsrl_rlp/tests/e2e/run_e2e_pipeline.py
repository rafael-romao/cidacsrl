# tests/e2e/run_e2e_pipeline.py

import os
import sys
import logging
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch

# Configuração de Logs para auditoria do processo
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CIDACS-RL-E2E")

def verify_and_validate_e2e_results():
    logger.info("Iniciando varredura e validação pós-execução nos volumes compartilhados...")
    
    # 1. Validação no Elasticsearch (Camada de Indexação)
    # Conecta ao cluster levantado na rede do Docker Compose
    es = Elasticsearch("http://elasticsearch:9200")
    es.indices.refresh(index="nascimentos_example_index")
    
    res = es.search(index="nascimentos_example_index", query={"match_all": {}})
    total_docs = res['hits']['total']['value']
    
    logger.info(f"Auditoria ES: Encontrados {total_docs} documentos no índice 'nascimentos_example_index'.")
    # O sample de nascimentos possui exatamente 100 registros
    assert total_docs == 100, f"Deveria ter 100 registros históricos indexados, mas encontrou: {total_docs}"

    # 2. Validação no Disco Local / Output (Camada de Linkage)
    # O output mapeado no spec deve gravar na pasta da fase de blocagem
    expected_output_path = "/mnt/storage/output/phase_1_fase_e2e_nome"
    
    assert os.path.exists(expected_output_path), (
        f"Erro Crítico: A pasta de resultados do linkage não foi gerada em: {expected_output_path}"
    )
    
    # Inicializa uma sessão local efêmera apenas para auditar o Parquet gravado
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("E2E-Output-Validation") \
        .getOrCreate()
        
    df_result = spark.read.parquet(expected_output_path)
    total_links = df_result.count()
    rows = df_result.limit(5).collect()
    spark.stop()

    logger.info(f"Auditoria Disco: Total de pares linkados gerados pelo motor: {total_links}")
    assert total_links > 0, "O pipeline rodou, mas nenhum par de matching foi gerado."
    
    logger.info("Exostose de Amostra dos Pares Identificados:")
    for i, row in enumerate(rows):
        logger.info(
            f"  [Par #{i+1}] Origem Internação ID: {row.source_codigo_internacao} "
            f"-> Candidato ES ID: {row.candidate_codigo_nascimento} | Score Final: {row.match_score}"
        )

if __name__ == "__main__":
    try:
        logger.info("=========================================================================")
        logger.info("               INICIANDO PIPELINE DE INTEGRAÇÃO FIM-A-FIM                ")
        logger.info("=========================================================================")
        
        # PASSO 1: Execução do Caso de Uso de Indexação via CLI
        # O indexador lerá os Parquets de 'data/input/nascimentos_example' de forma transparente
        logger.info("Passo 1/2: Disparando processo de Indexação em Massa (Bulk) no ES...")
        status_indexing = os.system(
            "python /app/cidacsrl_rlp/cli.py indexing "
            "--env-config /app/tests/e2e/configs/env_local.yml "
            "--spec-config /app/tests/e2e/configs/index_spec_local.yml"
        )
        if status_indexing != 0:
            raise RuntimeError("O Adaptador CLI reportou erro na execução do Caso de Uso de Indexação.")

        # PASSO 2: Execução do Caso de Uso de Linkage Sequencial via CLI
        # O linkage buscará os candidatos indexados cruzando com 'data/input/internacao_example'
        logger.info("Passo 2/2: Disparando motor de Record Linkage das fontes Parquet...")
        status_linkage = os.system(
            "python /app/cidacsrl_rlp/cli.py linkage "
            "--env-config /app/tests/e2e/configs/env_local.yml "
            "--spec-config /app/tests/e2e/configs/linkage_spec_local.yml"
        )
        if status_linkage != 0:
            raise RuntimeError("O Adaptador CLI reportou erro na execução do Caso de Uso de Linkage.")

        # PASSO 3: Validação das Regras e Asserts
        verify_and_validate_e2e_results()
        
        logger.info("=========================================================================")
        logger.info("     STATUS FINAL: SUCESSO")
        logger.info("=========================================================================")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Falha no Pipeline de Integração E2E: {e}")
        sys.exit(1)