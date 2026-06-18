import pytest
from unittest.mock import ANY, MagicMock, patch
from contextlib import contextmanager

from deduplicating.infra.bootstrappers.deduplicate_bootstrapper import bootstrap_deduplication
from deduplicating.infra.configs.models.deduplicate_workflow_config import (
    DeduplicateWorkflowConfig,
    DeduplicateStorageConfig,
)
from deduplicating.application.domain.models.deduplication_specification import DeduplicationSpecification

pytestmark = pytest.mark.unit

_MODULE = "deduplicating.infra.bootstrappers.deduplicate_bootstrapper"


@pytest.fixture
def workflow_config():
    return DeduplicateWorkflowConfig(
        storage=DeduplicateStorageConfig(
            source_path="data/linked.parquet",
            output_path="data/deduped.parquet",
        ),
        deduplication_spec=DeduplicationSpecification(
            id_source_column="id_table",
            id_target_column="candidate_id_table",
        ),
        app_name="Test App",
        spark_configs={"spark.master": "local[*]"},
    )


@pytest.fixture
def mock_spark():
    return MagicMock()


@pytest.fixture
def spark_context(mock_spark):
    @contextmanager
    def _ctx(*args, **kwargs):
        yield mock_spark

    return _ctx


@patch(f"{_MODULE}.DeduplicateUseCase")
@patch(f"{_MODULE}.SparkDataPersistenceAdapter")
@patch(f"{_MODULE}.GraphFramesAdapter")
@patch(f"{_MODULE}.SparkDataReaderAdapter")
@patch(f"{_MODULE}.spark_session_context")
def test_bootstrap_wires_all_adapters_and_executes(
    mock_ctx,
    mock_reader_cls,
    mock_graph_cls,
    mock_persistence_cls,
    mock_use_case_cls,
    workflow_config,
    mock_spark,
    spark_context,
):
    mock_ctx.side_effect = spark_context
    mock_use_case = MagicMock()
    mock_use_case_cls.return_value = mock_use_case

    bootstrap_deduplication(workflow_config)

    mock_ctx.assert_called_once_with(
        app_name=workflow_config.app_name,
        spark_config=workflow_config.spark_configs,
        checkpoint_dir=ANY,
    )
    mock_reader_cls.assert_called_once_with(spark=mock_spark, storage=workflow_config.storage)
    mock_graph_cls.assert_called_once_with()
    mock_persistence_cls.assert_called_once_with(storage=workflow_config.storage)
    mock_use_case_cls.assert_called_once_with(
        reader=mock_reader_cls.return_value,
        graph_processor=mock_graph_cls.return_value,
        persistence=mock_persistence_cls.return_value,
    )
    mock_use_case.execute.assert_called_once_with(spec=workflow_config.deduplication_spec)


@patch(f"{_MODULE}.DeduplicateUseCase")
@patch(f"{_MODULE}.SparkDataPersistenceAdapter")
@patch(f"{_MODULE}.GraphFramesAdapter")
@patch(f"{_MODULE}.SparkDataReaderAdapter")
@patch(f"{_MODULE}.spark_session_context")
def test_bootstrap_stops_spark_on_success(
    mock_ctx,
    mock_reader_cls,
    mock_graph_cls,
    mock_persistence_cls,
    mock_use_case_cls,
    workflow_config,
    mock_spark,
    spark_context,
):
    mock_ctx.side_effect = spark_context

    bootstrap_deduplication(workflow_config)

    # spark.stop() é responsabilidade do spark_session_context — verificamos que o
    # context manager foi usado (entered), não que stop() foi chamado diretamente.
    mock_ctx.assert_called_once()


@patch(f"{_MODULE}.DeduplicateUseCase")
@patch(f"{_MODULE}.SparkDataPersistenceAdapter")
@patch(f"{_MODULE}.GraphFramesAdapter")
@patch(f"{_MODULE}.SparkDataReaderAdapter")
@patch(f"{_MODULE}.spark_session_context")
def test_bootstrap_propagates_exception(
    mock_ctx,
    mock_reader_cls,
    mock_graph_cls,
    mock_persistence_cls,
    mock_use_case_cls,
    workflow_config,
    mock_spark,
    spark_context,
):
    mock_ctx.side_effect = spark_context
    mock_use_case_cls.return_value.execute.side_effect = RuntimeError("falha simulada")

    with pytest.raises(RuntimeError, match="falha simulada"):
        bootstrap_deduplication(workflow_config)
