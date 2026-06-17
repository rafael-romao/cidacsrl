import pytest
from unittest.mock import MagicMock, patch, call

from deduplicating.infra.adapters.outbound.graphframes_adapter import GraphFramesAdapter

pytestmark = pytest.mark.unit


@pytest.fixture
def df_pairs():
    df = MagicMock()

    df_src_ids = MagicMock()
    df_dst_ids = MagicMock()
    df_edges = MagicMock()

    df_src_ids_aliased = MagicMock()
    df_dst_ids_aliased = MagicMock()
    df_edges_src = MagicMock()
    df_edges_dst = MagicMock()

    src_col = MagicMock()
    dst_col = MagicMock()
    src_col.alias.return_value = df_src_ids_aliased
    dst_col.alias.return_value = df_dst_ids_aliased

    edge_src = MagicMock()
    edge_dst = MagicMock()
    edge_src.alias.return_value = df_edges_src
    edge_dst.alias.return_value = df_edges_dst

    df.select.side_effect = [df_src_ids, df_dst_ids, df_edges]
    df_src_ids.union.return_value = MagicMock()

    return df


@patch("deduplicating.infra.adapters.outbound.graphframes_adapter.GraphFrame")
@patch("deduplicating.infra.adapters.outbound.graphframes_adapter.F")
def test_find_clusters_instantiates_graphframe(mock_F, mock_graphframe_cls):
    df_pairs = MagicMock()
    df_vertices = MagicMock()
    df_edges_df = MagicMock()

    df_src = MagicMock()
    df_dst = MagicMock()
    df_src.union.return_value = MagicMock()
    df_src.union.return_value.distinct.return_value = df_vertices
    df_pairs.select.side_effect = [df_src, df_dst, df_edges_df]

    mock_graph = MagicMock()
    df_components = MagicMock()
    df_renamed = MagicMock()
    mock_graphframe_cls.return_value = mock_graph
    mock_graph.connectedComponents.return_value = df_components
    df_components.withColumnRenamed.return_value = df_renamed

    adapter = GraphFramesAdapter()
    result = adapter.find_clusters(df_pairs, "id_origem", "id_destino")

    mock_graphframe_cls.assert_called_once_with(df_vertices, df_edges_df)
    mock_graph.connectedComponents.assert_called_once()
    df_components.withColumnRenamed.assert_called_once_with("component", "cluster_id")
    assert result is df_renamed


@patch("deduplicating.infra.adapters.outbound.graphframes_adapter.GraphFrame")
@patch("deduplicating.infra.adapters.outbound.graphframes_adapter.F")
def test_find_clusters_uses_correct_column_aliases(mock_F, mock_graphframe_cls):
    df_pairs = MagicMock()
    df_src = MagicMock()
    df_dst = MagicMock()
    df_src.union.return_value = MagicMock()
    df_src.union.return_value.distinct.return_value = MagicMock()
    df_pairs.select.side_effect = [df_src, df_dst, MagicMock()]

    mock_graph = MagicMock()
    mock_graphframe_cls.return_value = mock_graph
    mock_graph.connectedComponents.return_value = MagicMock()
    mock_graph.connectedComponents.return_value.withColumnRenamed.return_value = MagicMock()

    adapter = GraphFramesAdapter()
    adapter.find_clusters(df_pairs, "col_src", "col_dst")

    col_calls = [c.args[0] for c in mock_F.col.call_args_list]
    assert "col_src" in col_calls
    assert "col_dst" in col_calls
