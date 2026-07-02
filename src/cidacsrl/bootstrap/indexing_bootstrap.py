import logging
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
    parse_execution_config,
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
    execution_config_data: Dict[str, Any] = None,
) -> Tuple[IndexDatasetUseCase, DatasetIndexingSpecification, SparkSession]:
    """Constrói e retorna o use case de indexação com todas as dependências injetadas.

    Instancia adapters de ingestão Spark, indexação ES e telemetria (log formatado
    e JSONL opcional, se audit_log_path configurado).

    Args:
        storage_config_data: Configuração de storage com source_path e source_format.
        indexing_spec_data: Especificação bruta do índice (nome, settings, colunas).
        es_config_data: Configuração de conexão com o Elasticsearch.
        spark_config_data: Configurações da SparkSession.
        execution_config_data: Configuração de execução para auditoria. Defaults to None.

    Returns:
        Tupla com (use_case, indexing_spec, spark_session).
    """
    logger.info("Building Indexing use case...")

    execution_config = parse_execution_config(execution_config_data or {})
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

    telemetry_adapters = [FormattedLogTelemetryAdapter()]
    if execution_config.audit_log_path:
        index_name = indexing_spec.index_config.name
        job_dir = (
            f"{execution_config.audit_log_path}"
            f"/indexing_{index_name}"
            f"/{execution_config.job_id}"
        )
        telemetry_adapters.append(JsonlIndexingTelemetryAdapter(file_path=f"{job_dir}/indexing.jsonl"))
    telemetry_adapter = CompositeIndexingTelemetryAdapter(telemetry_adapters)

    use_case = IndexDatasetUseCase(
        ingestion_port=ingestion_adapter,
        indexing_port=indexing_adapter,
        telemetry_port=telemetry_adapter,
    )

    return use_case, indexing_spec, spark
