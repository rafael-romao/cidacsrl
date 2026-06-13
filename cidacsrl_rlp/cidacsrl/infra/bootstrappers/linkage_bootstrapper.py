import logging
from typing import Any, Dict

from cidacsrl_rlp.shared.infra.spark.spark_factory import create_spark_session
from cidacsrl_rlp.cidacsrl.infra.configs.loader import (
    parse_sequential_linkage_specification, 
    parse_es_config,
    parse_source_storage_config,
    parse_output_storage_config,
    parse_execution_config
)

from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.client import get_es_client, validate_elasticsearch_schema
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_ingestion_adapter import SparkDataIngestionAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_persistence_adapter import SparkDataPersistenceAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_transformation_adapter import SparkDataTransformationAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_search_adapter import SparkESSearchAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_scoring_adapter import SparkScoringAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.executors import SingleSearchExecutor, MultiSearchExecutor


from cidacsrl_rlp.cidacsrl.application.use_cases.run_sequential_linkage import RunSequentialLinkageUseCase

logger = logging.getLogger(__name__)

def bootstrap_sequential_linkage(
    storage_config_data: Dict[str, Any],    
    execution_config_data: Dict[str, Any],  
    linkage_spec_data: Dict[str, Any],      
    es_config_data: Dict[str, Any],         
    spark_config_data: Dict[str, Any],       
):
    logger.info("Bootstrapping Sequential Linkage...")
    
    
    linkage_spec = parse_sequential_linkage_specification(linkage_spec_data)
    source_storage_config = parse_source_storage_config(storage_config_data)
    output_storage_config = parse_output_storage_config(storage_config_data)
    execution_config = parse_execution_config(execution_config_data)
    es_config = parse_es_config(es_config_data)

    
    try:
        logger.info("Validating Elasticsearch schema...")
        test_client = get_es_client(es_config, use_cache=False)
        validate_elasticsearch_schema(
            es_client=test_client, 
            index_name=linkage_spec.target_es_index, 
            required_columns=linkage_spec.get_required_target_columns()
        )
    except Exception as e:
        logger.error(f"Erro crítico durante a validação do schema do Elasticsearch: {e}")
        raise e

    strategy_name = es_config.get("search_strategy", "multisearch").lower()
    
    if strategy_name == "single":
        logger.info("Motor de busca configurado para Single - cada consulta é executada individualmente.")
        search_executor = SingleSearchExecutor()
    elif strategy_name == "multisearch":
        logger.info("Motor de busca configurado para MultiSearch - consultas são executadas em lote otimizado.")
        search_executor = MultiSearchExecutor()
    else:
        raise ValueError(f"Estratégia de busca desconhecida: '{strategy_name}'. Escolha 'single' ou 'multisearch'.")

    spark_session = create_spark_session(
        app_name=f"CIDACS-RL Record Linkage - {linkage_spec.source_table}_{linkage_spec.target_es_index}", 
        spark_config=spark_config_data.get("spark_configs", {})
    )

    try:       
        ingestion_adapter = SparkDataIngestionAdapter(spark_session=spark_session, config=source_storage_config)
        persistence_adapter = SparkDataPersistenceAdapter(spark_session=spark_session, config=output_storage_config)
        transformation_adapter = SparkDataTransformationAdapter()
        
        errors = ingestion_adapter.check_health(linkage_spec.source_table)
        if errors:
            raise ValueError(f"Falha de Infraestrutura: {errors}")


        search_adapter = SparkESSearchAdapter(
            index_name=linkage_spec.target_es_index, 
            es_config=es_config,
            search_executor=search_executor
        )
        
        scoring_adapter = SparkScoringAdapter()

        use_case = RunSequentialLinkageUseCase(
            ingestion_port=ingestion_adapter,
            persistence_port=persistence_adapter,
            transformation_port=transformation_adapter,
            get_candidates_port=search_adapter,
            scoring_port=scoring_adapter
        )

        use_case.execute(config=linkage_spec)
        logger.info("Linkage Execution finished successfully.")
        
    except Exception as e:
        logger.error(f"Erro durante a execução do linkage: {e}")
        raise e
    finally:
        spark_session.stop()
        logger.info("Spark session stopped.")