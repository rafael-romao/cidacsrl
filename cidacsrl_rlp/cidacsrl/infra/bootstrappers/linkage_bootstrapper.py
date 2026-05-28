from cidacsrl_rlp.cidacsrl.infra.configs.loader import (
    load_linkage_workflow_config,
    load_sequential_blocking_workflow_config,
    load_es_config,
)
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.client import get_es_client
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_repository_adapter import SparkDataRepositoryAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_search_adapter import SparkESSearchAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_scoring_adapter import SparkScoringAdapter
from cidacsrl_rlp.cidacsrl.application.use_cases.run_sequential_linkage_workflow import RunSequentialLinkageWorkflowUseCase
from dataclasses import asdict
from typing import Any
import logging

logger = logging.getLogger(__name__)

def bootstrap_sequential_linkage(config_path: str, spark_session: Any) -> None:
    # 1. Carrega as configurações (YAML)
    workflow_config = load_linkage_workflow_config(config_path)
    blocking_config = load_sequential_blocking_workflow_config(workflow_config.linkage_config_path)
    es_config       = load_es_config(workflow_config.es_config_path)

    # Sanity Check de Conectividade com Elasticsearch
    test_client = get_es_client(es_config, use_cache=False)
    if not test_client:
        raise ConnectionError("Falha crítica de conectividade com o Elasticsearch antes da inicialização.")

    # 2. Instancia a Infraestrutura Unificada
    spark_adapter = SparkDataRepositoryAdapter(spark_session=spark_session, env_config=workflow_config)
    
    # 3. Sanity Check de Saúde da Infraestrutura
    errors = spark_adapter.check_health(blocking_config.source_table, blocking_config.target_es_index)
    if errors:
        raise ValueError(f"Falha de Infraestrutura: {errors}")

    # 4. Instancia as dependências restantes de busca e scoring
    search_adapter = SparkESSearchAdapter(index_name=blocking_config.target_es_index, es_config=es_config)
    scoring_adapter = SparkScoringAdapter()

    # 5. Injeta e Orquestra o Caso de Uso
    use_case = RunSequentialLinkageWorkflowUseCase(
        ingestion_port=spark_adapter,
        persistence_port=spark_adapter,
        transformation_port=spark_adapter,
        get_candidates_port=search_adapter,
        scoring_port=scoring_adapter
    )
    
    use_case.execute(blocking_config)    
    logger.info("Pipeline de Record Linkage finalizado com sucesso.")