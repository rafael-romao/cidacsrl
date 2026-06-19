from unittest.mock import MagicMock, ANY
from pyspark.sql import DataFrame

from core.application.use_cases.index_dataset_use_case import IndexDatasetUseCase
from core.application.ports.outbound.data_ingestion_port import DataIngestionPort
from core.application.ports.outbound.data_indexing_port import DataIndexingPort
from core.application.ports.outbound.indexing_telemetry_port import IndexingTelemetryPort
from cidacsrl.domain.indexing.indexing_specification import DatasetIndexingSpecification, IndexColumnConfig, IndexSettingsConfig


def _make_spec():
    mock_spec = MagicMock(spec=DatasetIndexingSpecification)
    mock_source_config = MagicMock()
    mock_source_config.source_table = "tabela_origem"
    mock_spec.source_config = mock_source_config
    mock_index_config = MagicMock(spec=IndexSettingsConfig)
    mock_index_config.name = "idx_nascimentos"
    mock_spec.index_config = mock_index_config
    mock_spec.index_columns = [
        IndexColumnConfig(name="codigo_nascimento", type="keyword"),
        IndexColumnConfig(name="nome_completo", type="text"),
    ]
    return mock_spec


# =========================================================================
# 1. CASO DE USO: EXECUÇÃO COMPLETA COM SELEÇÃO DE COLUNAS (HAPPY PATH)
# =========================================================================

def test_index_dataset_use_case_execution_success():
    mock_ingestion_port = MagicMock(spec=DataIngestionPort)
    mock_indexing_port = MagicMock(spec=DataIndexingPort)
    mock_telemetry = MagicMock(spec=IndexingTelemetryPort)

    mock_spec = _make_spec()

    mock_df_source = MagicMock(spec=DataFrame)
    mock_df_filtered = MagicMock(spec=DataFrame)
    mock_df_source.select.return_value = mock_df_filtered
    mock_ingestion_port.read_all.return_value = mock_df_source

    use_case = IndexDatasetUseCase(
        ingestion_port=mock_ingestion_port,
        indexing_port=mock_indexing_port,
        telemetry_port=mock_telemetry,
    )

    use_case.execute(spec=mock_spec)

    mock_indexing_port.ensure_index_with_mapping.assert_called_once_with(mock_spec)
    mock_ingestion_port.read_all.assert_called_once_with(table_name="tabela_origem")
    mock_df_source.select.assert_called_once_with("codigo_nascimento", "nome_completo")
    mock_indexing_port.index_dataframe.assert_called_once_with(
        df=mock_df_filtered,
        spec=mock_spec,
    )


def test_index_dataset_use_case_emits_telemetry():
    mock_ingestion_port = MagicMock(spec=DataIngestionPort)
    mock_indexing_port = MagicMock(spec=DataIndexingPort)
    mock_telemetry = MagicMock(spec=IndexingTelemetryPort)

    mock_spec = _make_spec()

    mock_df_source = MagicMock(spec=DataFrame)
    mock_df_source.select.return_value = MagicMock(spec=DataFrame)
    mock_ingestion_port.read_all.return_value = mock_df_source

    use_case = IndexDatasetUseCase(
        ingestion_port=mock_ingestion_port,
        indexing_port=mock_indexing_port,
        telemetry_port=mock_telemetry,
    )

    use_case.execute(spec=mock_spec)

    mock_telemetry.log_indexing_start.assert_called_once_with(
        source_table="tabela_origem",
        index_name="idx_nascimentos",
        column_count=2,
    )
    mock_telemetry.log_index_ensured.assert_called_once_with(
        index_name="idx_nascimentos", duration=ANY
    )
    mock_telemetry.log_indexing_completion.assert_called_once_with(
        source_table="tabela_origem",
        index_name="idx_nascimentos",
        total_duration=ANY,
    )
