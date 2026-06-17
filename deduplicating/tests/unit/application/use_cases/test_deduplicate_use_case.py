import pytest
from unittest.mock import MagicMock

from deduplicating.application.domain.models.deduplication_specification import DeduplicationSpecification
from deduplicating.application.ports.outbound.data_reader_port import DataReaderPort
from deduplicating.application.ports.outbound.graph_processing_port import GraphProcessingPort
from deduplicating.application.ports.outbound.data_persistence_port import DataPersistencePort
from deduplicating.application.use_cases.deduplicate_use_case import DeduplicateUseCase

pytestmark = pytest.mark.unit


@pytest.fixture
def spec():
    return DeduplicationSpecification(
        id_source_column="id_origem",
        id_target_column="id_destino",
        output_group_id_column="grupo_id",
    )


@pytest.fixture
def mock_ports():
    return (
        MagicMock(spec=DataReaderPort),
        MagicMock(spec=GraphProcessingPort),
        MagicMock(spec=DataPersistencePort),
    )


@pytest.fixture
def chained_dataframes():
    df_pairs = MagicMock()
    df_clusters = MagicMock()
    df_joined = MagicMock()
    df_dropped = MagicMock()
    df_renamed = MagicMock()

    df_pairs.join.return_value = df_joined
    df_joined.drop.return_value = df_dropped
    df_dropped.withColumnRenamed.return_value = df_renamed

    return df_pairs, df_clusters, df_joined, df_dropped, df_renamed


def test_execute_calls_all_ports_in_order(spec, mock_ports, chained_dataframes):
    reader, graph_processor, persistence = mock_ports
    df_pairs, df_clusters, _, _, df_renamed = chained_dataframes

    reader.read_linked_pairs.return_value = df_pairs
    graph_processor.find_clusters.return_value = df_clusters

    use_case = DeduplicateUseCase(reader, graph_processor, persistence)
    use_case.execute(spec)

    reader.read_linked_pairs.assert_called_once()
    graph_processor.find_clusters.assert_called_once()
    persistence.save.assert_called_once_with(df_renamed)


def test_execute_passes_spec_columns_to_graph_processor(spec, mock_ports, chained_dataframes):
    reader, graph_processor, persistence = mock_ports
    df_pairs, df_clusters, _, _, _ = chained_dataframes

    reader.read_linked_pairs.return_value = df_pairs
    graph_processor.find_clusters.return_value = df_clusters

    DeduplicateUseCase(reader, graph_processor, persistence).execute(spec)

    graph_processor.find_clusters.assert_called_once_with(
        df_pairs=df_pairs,
        id_source_column="id_origem",
        id_target_column="id_destino",
    )


def test_execute_drops_id_column_after_join(spec, mock_ports, chained_dataframes):
    reader, graph_processor, persistence = mock_ports
    df_pairs, df_clusters, df_joined, _, _ = chained_dataframes

    reader.read_linked_pairs.return_value = df_pairs
    graph_processor.find_clusters.return_value = df_clusters

    DeduplicateUseCase(reader, graph_processor, persistence).execute(spec)

    df_joined.drop.assert_called_once_with("id")


def test_execute_renames_cluster_id_to_spec_output_column(spec, mock_ports, chained_dataframes):
    reader, graph_processor, persistence = mock_ports
    df_pairs, df_clusters, _, df_dropped, _ = chained_dataframes

    reader.read_linked_pairs.return_value = df_pairs
    graph_processor.find_clusters.return_value = df_clusters

    DeduplicateUseCase(reader, graph_processor, persistence).execute(spec)

    df_dropped.withColumnRenamed.assert_called_once_with("cluster_id", "grupo_id")


def test_execute_propagates_reader_exception(spec, mock_ports):
    reader, graph_processor, persistence = mock_ports
    reader.read_linked_pairs.side_effect = RuntimeError("falha de leitura")

    with pytest.raises(RuntimeError, match="falha de leitura"):
        DeduplicateUseCase(reader, graph_processor, persistence).execute(spec)

    graph_processor.find_clusters.assert_not_called()
    persistence.save.assert_not_called()


def test_execute_propagates_graph_processor_exception(spec, mock_ports, chained_dataframes):
    reader, graph_processor, persistence = mock_ports
    df_pairs, _, _, _, _ = chained_dataframes

    reader.read_linked_pairs.return_value = df_pairs
    graph_processor.find_clusters.side_effect = RuntimeError("falha no grafo")

    with pytest.raises(RuntimeError, match="falha no grafo"):
        DeduplicateUseCase(reader, graph_processor, persistence).execute(spec)

    persistence.save.assert_not_called()
