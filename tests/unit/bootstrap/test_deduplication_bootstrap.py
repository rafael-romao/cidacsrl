import pytest
from unittest.mock import ANY, MagicMock, patch

from cidacsrl.config.models.storage_config import SourceStorageConfig, OutputStorageConfig
from cidacsrl.bootstrap.deduplication_bootstrap import build_deduplication_use_case
from cidacsrl.config.models.dedup_workflow_config import DeduplicateWorkflowConfig
from cidacsrl.domain.deduplication.deduplication_specification import DeduplicationSpecification

pytestmark = pytest.mark.unit

_MODULE = "cidacsrl.bootstrap.deduplication_bootstrap"


@pytest.fixture
def workflow_config():
    return DeduplicateWorkflowConfig(
        source_storage=SourceStorageConfig(source_path="data/linked.parquet"),
        output_storage=OutputStorageConfig(output_path="data/deduped.parquet"),
        deduplication_spec=DeduplicationSpecification(
            id_source_column="id_table",
            id_target_column="candidate_id_table",
        ),
        app_name="Test App",
        spark_configs={"spark.master": "local[*]"},
    )


@patch(f"{_MODULE}.DeduplicateUseCase")
@patch(f"{_MODULE}.CompositeDeduplicationTelemetryAdapter")
@patch(f"{_MODULE}.JsonlDeduplicationTelemetryAdapter")
@patch(f"{_MODULE}.FormattedLogDeduplicationTelemetryAdapter")
@patch(f"{_MODULE}.SparkDataPersistenceAdapter")
@patch(f"{_MODULE}.GraphFramesAdapter")
@patch(f"{_MODULE}.SparkDataReaderAdapter")
@patch(f"{_MODULE}.create_spark_session")
def test_build_wires_all_adapters(
    mock_create_spark,
    mock_reader_cls,
    mock_graph_cls,
    mock_persistence_cls,
    mock_formatted_telemetry_cls,
    mock_jsonl_telemetry_cls,
    mock_composite_cls,
    mock_use_case_cls,
    workflow_config,
):
    mock_spark = MagicMock()
    mock_create_spark.return_value = mock_spark
    mock_use_case = MagicMock()
    mock_use_case_cls.return_value = mock_use_case

    returned_use_case, returned_spark = build_deduplication_use_case(workflow_config)

    mock_create_spark.assert_called_once_with(
        app_name=workflow_config.app_name,
        spark_config=workflow_config.spark_configs,
        checkpoint_dir=ANY,
    )
    mock_reader_cls.assert_called_once_with(spark=mock_spark, storage=workflow_config.source_storage)
    mock_graph_cls.assert_called_once_with()
    mock_persistence_cls.assert_called_once_with(storage=workflow_config.output_storage)
    mock_formatted_telemetry_cls.assert_called_once_with()
    mock_use_case_cls.assert_called_once_with(
        reader=mock_reader_cls.return_value,
        graph_processor=mock_graph_cls.return_value,
        persistence=mock_persistence_cls.return_value,
        telemetry=mock_composite_cls.return_value,
    )
    assert returned_use_case is mock_use_case
    assert returned_spark is mock_spark
    mock_use_case.execute.assert_not_called()


@patch(f"{_MODULE}.DeduplicateUseCase")
@patch(f"{_MODULE}.CompositeDeduplicationTelemetryAdapter")
@patch(f"{_MODULE}.JsonlDeduplicationTelemetryAdapter")
@patch(f"{_MODULE}.FormattedLogDeduplicationTelemetryAdapter")
@patch(f"{_MODULE}.SparkDataPersistenceAdapter")
@patch(f"{_MODULE}.GraphFramesAdapter")
@patch(f"{_MODULE}.SparkDataReaderAdapter")
@patch(f"{_MODULE}.create_spark_session")
def test_build_returns_spark_session(
    mock_create_spark,
    mock_reader_cls,
    mock_graph_cls,
    mock_persistence_cls,
    mock_formatted_telemetry_cls,
    mock_jsonl_telemetry_cls,
    mock_composite_cls,
    mock_use_case_cls,
    workflow_config,
):
    mock_spark = MagicMock()
    mock_create_spark.return_value = mock_spark

    _, returned_spark = build_deduplication_use_case(workflow_config)

    assert returned_spark is mock_spark


@patch(f"{_MODULE}.DeduplicateUseCase")
@patch(f"{_MODULE}.CompositeDeduplicationTelemetryAdapter")
@patch(f"{_MODULE}.JsonlDeduplicationTelemetryAdapter")
@patch(f"{_MODULE}.FormattedLogDeduplicationTelemetryAdapter")
@patch(f"{_MODULE}.SparkDataPersistenceAdapter")
@patch(f"{_MODULE}.GraphFramesAdapter")
@patch(f"{_MODULE}.SparkDataReaderAdapter")
@patch(f"{_MODULE}.create_spark_session")
def test_build_propagates_exception_from_adapter(
    mock_create_spark,
    mock_reader_cls,
    mock_graph_cls,
    mock_persistence_cls,
    mock_formatted_telemetry_cls,
    mock_jsonl_telemetry_cls,
    mock_composite_cls,
    mock_use_case_cls,
    workflow_config,
):
    mock_create_spark.return_value = MagicMock()
    mock_reader_cls.side_effect = RuntimeError("falha simulada")

    with pytest.raises(RuntimeError, match="falha simulada"):
        build_deduplication_use_case(workflow_config)
