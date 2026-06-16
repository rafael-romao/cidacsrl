import logging
from typing import Any, Dict

from core.cidacsrl.infra.spark.spark_factory import create_spark_session
from core.cidacsrl.infra.configs.loader import (
    parse_sequential_linkage_specification, 
    parse_es_config,
    parse_source_storage_config,
    parse_output_storage_config,
    parse_execution_config
)

from core.cidacsrl.infra.adapters.outbound.elasticsearch.client import get_es_client, validate_elasticsearch_schema
from core.cidacsrl.infra.adapters.outbound.spark_data_ingestion_adapter import SparkDataIngestionAdapter
from core.cidacsrl.infra.adapters.outbound.spark_data_persistence_adapter import SparkDataPersistenceAdapter
from core.cidacsrl.infra.adapters.outbound.spark_data_transformation_adapter import SparkDataTransformationAdapter
from core.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_search_adapter import SparkESSearchAdapter
from core.cidacsrl.infra.adapters.outbound.spark_scoring_adapter import SparkScoringAdapter
from core.cidacsrl.infra.adapters.outbound.elasticsearch.executors import SingleSearchExecutor, MultiSearchExecutor
from core.cidacsrl.infra.adapters.outbound.json_execution_tracking_adapter import JSONExecutionTrackingAdapter

from core.cidacsrl.application.services.work_unit_orchestrator import WorkUnitOrchestrator
from core.cidacsrl.application.use_cases.record_linkage_use_case import RecordLinkageUseCase

logger = logging.getLogger(__name__)

def bootstrap_sequential_linkage(
    storage_config_data: Dict[str, Any],    
    execution_config_data: Dict[str, Any],  
    linkage_spec_data: Dict[str, Any],      
    es_config_data: Dict[str, Any],         
    spark_config_data: Dict[str, Any],       
):
    logger.info("Bootstrapping Sequential Linkage...")
    
    # 1. Parseamento declarativo das estruturas de configuração e especificações técnicas
    linkage_spec = parse_sequential_linkage_specification(linkage_spec_data)
    source_storage_config = parse_source_storage_config(storage_config_data)
    output_storage_config = parse_output_storage_config(storage_config_data)
    execution_config = parse_execution_config(execution_config_data)
    es_config = parse_es_config(es_config_data)

    job_id = execution_config.job_id
    logger.info(f"Contexto operacional resolvido com sucesso. Job ID ativo: '{job_id}'")

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

    # 3. Determinação do motor de busca (Single vs MultiSearch bulk queries)
    strategy_name = es_config.get("search_strategy", "multisearch").lower()
    if strategy_name == "single":
        search_executor = SingleSearchExecutor()
    elif strategy_name == "multisearch":
        search_executor = MultiSearchExecutor()
    else:
        raise ValueError(f"Estratégia de busca Elasticsearch desconhecida: '{strategy_name}'.")

    # 4. Inicialização do contexto computacional distribuído do Apache Spark
    spark_session = create_spark_session(
        app_name=f"CIDACS-RL Linkage - {linkage_spec.source_table}_{linkage_spec.target_es_index}", 
        spark_config=spark_config_data.get("spark_configs", {})
    )

    try:       
        
        tracking_adapter = JSONExecutionTrackingAdapter(
            tracking_directory=execution_config.audit_log_path
        )

        ingestion_adapter = SparkDataIngestionAdapter(
            spark_session=spark_session, 
            storage_config=source_storage_config
        )
        
        persistence_adapter = SparkDataPersistenceAdapter(
            output_config=output_storage_config
        )
        
        transformation_adapter = SparkDataTransformationAdapter()
        
        # 6. Validação física do estado do storage de dados
        errors = ingestion_adapter.check_health(linkage_spec.source_table)
        if errors:
            raise ValueError(f"Falha de Infraestrutura: {errors}")


        search_adapter = SparkESSearchAdapter(
            index_name=linkage_spec.target_es_index, 
            es_config=es_config,
            search_executor=search_executor
        )
        
        scoring_adapter = SparkScoringAdapter()

        
        orchestrator = WorkUnitOrchestrator(
            ingestion_port=ingestion_adapter,
            tracking_port=tracking_adapter
        )

        use_case = RecordLinkageUseCase(
            orchestrator=orchestrator,
            persistence_port=persistence_adapter,
            transformation_port=transformation_adapter,
            get_candidates_port=search_adapter,
            scoring_port=scoring_adapter,
            tracking_port=tracking_adapter
        )

        use_case.execute(
            specification=linkage_spec, 
            job_id=job_id,
            execution_config=execution_config
        )
        
        logger.info("Linkage Execution finished successfully.")
        
    except Exception as e:
        logger.error(f"Erro durante a execução do linkage: {e}")
        raise e
    finally:
        spark_session.stop()
        logger.info("Spark session stopped.")