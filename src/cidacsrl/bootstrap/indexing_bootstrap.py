import logging
from datetime import datetime
from typing import Any, Dict, Tuple

from pyspark.sql import SparkSession

from cidacsrl.adapters.outbound.elasticsearch.spark_es_indexing_adapter import (
    SparkESIndexingAdapter,
)
from cidacsrl.adapters.outbound.spark.data_ingestion_adapter import (
    SparkDataIngestionAdapter,
)
from cidacsrl.adapters.outbound.spark.spark_factory import create_spark_session
from cidacsrl.adapters.outbound.telemetry.composite_linkage_telemetry_adapter import (
    CompositeIndexingTelemetryAdapter,
)
from cidacsrl.adapters.outbound.telemetry.formatted_log_telemetry_adapter import (
    FormattedLogTelemetryAdapter,
)
from cidacsrl.adapters.outbound.telemetry.jsonl_telemetry_adapter import (
    JsonlIndexingTelemetryAdapter,
)
from cidacsrl.application.indexing.index_dataset_use_case import (
    IndexDatasetUseCase,
)
from cidacsrl.config.loader import (
    parse_dataset_indexing_specification,
    parse_es_config,
)
from cidacsrl.config.models.storage_config import SourceStorageConfig
from cidacsrl.domain.indexing.indexing_specification import (
    DatasetIndexingSpecification,
)

logger = logging.getLogger("Bootstrap: Elasticsearch Indexing")


def build_indexing_use_case(
    storage_config_data: Dict[str, Any],
    indexing_spec_data: Dict[str, Any],
    es_config_data: Dict[str, Any],
    spark_config_data: Dict[str, Any],
) -> Tuple[IndexDatasetUseCase, DatasetIndexingSpecification, SparkSession]:
    logger.info("Building Indexing use case...")

    source_config = SourceStorageConfig(
        source_path=storage_config_data["source_path"],
        source_format=storage_config_data.get("source_format", "parquet"),
    )
    es_config = parse_es_config(es_config_data)
    indexing_spec = parse_dataset_indexing_specification(indexing_spec_data)

    spark = create_spark_session(
        app_name=(
            f"CIDACS-RL Indexing Table {indexing_spec.source_config.source_table}"
            f" as {indexing_spec.index_config.name}"
        ),
        spark_config=spark_config_data,
    )

    ingestion_adapter = SparkDataIngestionAdapter(spark_session=spark, storage_config=source_config)
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

    return use_case, indexing_spec, spark
