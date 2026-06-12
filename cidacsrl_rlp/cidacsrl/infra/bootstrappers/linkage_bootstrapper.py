import logging
from typing import Any, Dict

from cidacsrl_rlp.shared.infra.spark.spark_factory import create_spark_session
from cidacsrl_rlp.cidacsrl.infra.configs.models.storage_config import SourceStorageConfig, OutputStorageConfig
from cidacsrl_rlp.cidacsrl.infra.configs.loader import parse_sequential_linkage_specification, parse_es_config

from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.client import get_es_client, validate_elasticsearch_schema
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_ingestion_adapter import SparkDataIngestionAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_persistence_adapter import SparkDataPersistenceAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_transformation_adapter import SparkDataTransformationAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_search_adapter import SparkESSearchAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_scoring_adapter import SparkScoringAdapter

from cidacsrl_rlp.cidacsrl.application.use_cases.run_sequential_linkage import RunSequentialLinkageUseCase

logger = logging.getLogger(__name__)

def bootstrap_sequential_linkage(
    storage_config_data: Dict[str, Any],
    linkage_spec_data: Dict[str, Any],
    es_config_data: Dict[str, Any],
    spark_config_data: Dict[str, Any]
) -> None:
    logger.info("Initializing Linkage Bootstrapper...")

    source_config = SourceStorageConfig(
        source_data_path=storage_config_data["source_data_path"],
        source_data_format=storage_config_data.get("source_data_format", "parquet")
    )
    output_config = OutputStorageConfig(
        output_data_path=storage_config_data["output_data_path"],
        output_data_format=storage_config_data.get("output_data_format", "parquet")
    )
    
    linkage_spec = parse_sequential_linkage_specification(linkage_spec_data)
    es_config = parse_es_config(es_config_data)

    test_client = get_es_client(es_config, use_cache=False)
    validate_elasticsearch_schema(test_client, linkage_spec.target_es_index, linkage_spec.get_required_target_columns())

    spark_session = create_spark_session(
        app_name=f"CIDACS-RL Record Linkage - {linkage_spec.source_table}_{linkage_spec.target_es_index}", 
        spark_config=spark_config_data
    )

    try:        
        ingestion_adapter = SparkDataIngestionAdapter(spark_session=spark_session, config=source_config)
        persistence_adapter = SparkDataPersistenceAdapter(spark_session=spark_session, config=output_config)
        transformation_adapter = SparkDataTransformationAdapter()
        
        errors = ingestion_adapter.check_health(linkage_spec.source_table)
        if errors:
            raise ValueError(f"Falha de Infraestrutura: {errors}")

        search_adapter = SparkESSearchAdapter(index_name=linkage_spec.target_es_index, es_config=es_config)
        scoring_adapter = SparkScoringAdapter()

        use_case = RunSequentialLinkageUseCase(
            ingestion_port=ingestion_adapter,
            persistence_port=persistence_adapter,
            transformation_port=transformation_adapter,
            get_candidates_port=search_adapter,
            scoring_port=scoring_adapter
        )

        use_case.execute(config=linkage_spec)
        logger.info("Linkage Use Case executed successfully.")
    finally:
        spark_session.stop()