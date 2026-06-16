# cidacsrl_rlp/cidacsrl/infra/bootstrappers/indexing_bootstrapper.py
import logging
from typing import Any, Dict

from cidacsrl.cidacsrl.infra.spark.spark_factory import create_spark_session
from cidacsrl.cidacsrl.infra.configs.models.storage_config import SourceStorageConfig
from cidacsrl.cidacsrl.infra.configs.loader import parse_dataset_indexing_specification, parse_es_config

from cidacsrl.cidacsrl.infra.adapters.outbound.spark_data_ingestion_adapter import SparkDataIngestionAdapter
from cidacsrl.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_indexing_adapter import SparkESIndexingAdapter
from cidacsrl.cidacsrl.application.use_cases.index_dataset_use_case import IndexDatasetUseCase

logger = logging.getLogger(__name__)

def bootstrap_elasticsearch_indexing(
    storage_config_data: Dict[str, Any],
    indexing_spec_data: Dict[str, Any],
    es_config_data: Dict[str, Any],
    spark_config_data: Dict[str, Any]
) -> None:
    logger.info("Initializing Indexing Bootstrapper...")

    source_config = SourceStorageConfig(
        source_path=storage_config_data["source_path"],
        source_format=storage_config_data.get("source_format", "parquet")
    )
    es_config = parse_es_config(es_config_data)
    indexing_spec = parse_dataset_indexing_specification(indexing_spec_data)

    
    spark_session = create_spark_session(
        app_name=f"CIDACS-RL Indexing Table {indexing_spec.source_config.source_table} as {indexing_spec.index_config.name}", 
        spark_config=spark_config_data
    )

    try:        
        ingestion_adapter = SparkDataIngestionAdapter(spark_session=spark_session, config=source_config)
        indexing_adapter = SparkESIndexingAdapter(es_config=es_config)

        use_case = IndexDatasetUseCase(
            ingestion_port=ingestion_adapter, 
            indexing_port=indexing_adapter
        )
        
        use_case.execute(
            spec=indexing_spec,
        )
        logger.info("Indexing Use Case executed successfully.")
    finally:
        spark_session.stop()