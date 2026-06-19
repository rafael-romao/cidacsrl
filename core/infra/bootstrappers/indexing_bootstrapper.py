import logging
from datetime import datetime
from typing import Any, Dict

from core.infra.spark.spark_factory import create_spark_session
from cidacsrl.config.models.storage_config import SourceStorageConfig
from cidacsrl.config.loader import parse_dataset_indexing_specification, parse_es_config

from core.infra.adapters.outbound.spark_data_ingestion_adapter import SparkDataIngestionAdapter
from core.infra.adapters.outbound.elasticsearch.spark_es_indexing_adapter import SparkESIndexingAdapter
from core.infra.adapters.outbound.formatted_log_telemetry_adapter import FormattedLogTelemetryAdapter
from core.infra.adapters.outbound.jsonl_telemetry_adapter import JsonlIndexingTelemetryAdapter
from core.infra.adapters.outbound.composite_telemetry_adapter import CompositeIndexingTelemetryAdapter
from cidacsrl.application.indexing.index_dataset_use_case import IndexDatasetUseCase

logger = logging.getLogger("Bootstrapper: Elasticsearch Indexing")

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
        ingestion_adapter = SparkDataIngestionAdapter(spark_session=spark_session, storage_config=source_config)
        indexing_adapter = SparkESIndexingAdapter(es_config=es_config)

        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        telemetry_dir = storage_config_data.get("telemetry_path", "./telemetry")
        jsonl_path = f"{telemetry_dir}/{indexing_spec.index_config.name}_{run_ts}_indexing_telemetry.jsonl"
        telemetry_adapter = CompositeIndexingTelemetryAdapter([
            FormattedLogTelemetryAdapter(),
            JsonlIndexingTelemetryAdapter(file_path=jsonl_path),
        ])

        use_case = IndexDatasetUseCase(
            ingestion_port=ingestion_adapter,
            indexing_port=indexing_adapter,
            telemetry_port=telemetry_adapter,
        )
        
        use_case.execute(
            spec=indexing_spec,
        )
        logger.info("Indexing Use Case executed successfully.")
    finally:
        spark_session.stop()