import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple

from pyspark.sql import SparkSession

from cidacsrl.adapters.outbound.graph.graphframes_adapter import (
    GraphFramesAdapter,
)
from cidacsrl.adapters.outbound.spark.data_reader_adapter import (
    SparkDataReaderAdapter,
)
from cidacsrl.adapters.outbound.spark.dedup_data_persistence_adapter import (
    SparkDataPersistenceAdapter,
)
from cidacsrl.adapters.outbound.spark.spark_factory import create_spark_session
from cidacsrl.adapters.outbound.telemetry.composite_dedup_telemetry_adapter import (
    CompositeDeduplicationTelemetryAdapter,
)
from cidacsrl.adapters.outbound.telemetry.formatted_log_dedup_telemetry_adapter import (
    FormattedLogDeduplicationTelemetryAdapter,
)
from cidacsrl.adapters.outbound.telemetry.jsonl_dedup_telemetry_adapter import (
    JsonlDeduplicationTelemetryAdapter,
)
from cidacsrl.application.deduplication.deduplicate_use_case import (
    DeduplicateUseCase,
)
from cidacsrl.config.models.dedup_workflow_config import (
    DeduplicateWorkflowConfig,
)

logger = logging.getLogger("Bootstrap: Deduplication")

_CHECKPOINT_DIR = "/tmp/cidacsrl_dedup_checkpoint"


def build_deduplication_use_case(
    config: DeduplicateWorkflowConfig,
) -> Tuple[DeduplicateUseCase, SparkSession]:
    logger.info("Building Deduplication use case...")

    spark = create_spark_session(
        app_name=config.app_name,
        spark_config=config.spark_configs,
        checkpoint_dir=_CHECKPOINT_DIR,
    )

    reader = SparkDataReaderAdapter(spark=spark, storage=config.source_storage)
    graph_processor = GraphFramesAdapter()
    persistence = SparkDataPersistenceAdapter(storage=config.output_storage)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(config.output_storage.output_path).parent
    jsonl_path = str(output_dir / f"{run_id}_dedup_telemetry.jsonl")
    telemetry = CompositeDeduplicationTelemetryAdapter([
        FormattedLogDeduplicationTelemetryAdapter(),
        JsonlDeduplicationTelemetryAdapter(file_path=jsonl_path, run_id=run_id),
    ])

    use_case = DeduplicateUseCase(
        reader=reader,
        graph_processor=graph_processor,
        persistence=persistence,
        telemetry=telemetry,
    )

    return use_case, spark
