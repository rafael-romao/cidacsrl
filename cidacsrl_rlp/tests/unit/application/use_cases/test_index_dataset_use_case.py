import pytest
from unittest.mock import MagicMock
from pyspark.sql import DataFrame

from cidacsrl_rlp.cidacsrl.application.use_cases.index_dataset_use_case import IndexDatasetUseCase
from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_ingestion_port import DataIngestionPort
from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_indexing_port import DataIndexingPort
from cidacsrl_rlp.cidacsrl.domain.models.indexing_specification import DatasetIndexingSpecification, IndexColumnConfig


# =========================================================================
# 1. CASO DE USO: EXECUÇÃO COMPLETA COM SELEÇÃO DE COLUNAS (HAPPY PATH)
# =========================================================================

def test_index_dataset_use_case_execution_success():
    mock_ingestion_port = MagicMock(spec=DataIngestionPort)
    mock_indexing_port = MagicMock(spec=DataIndexingPort)
    
    # CORREÇÃO: Montagem explícita dos nós de mock aninhados
    mock_spec = MagicMock(spec=DatasetIndexingSpecification)
    mock_index_config = MagicMock()
    mock_index_config.name = "nascimentos_test"
    mock_spec.index_config = mock_index_config
    
    mock_spec.index_columns = [
        IndexColumnConfig(name="codigo_nascimento", type="keyword"),
        IndexColumnConfig(name="nome_completo", type="text")
    ]
    
    mock_df_source = MagicMock(spec=DataFrame)
    mock_df_filtered = MagicMock(spec=DataFrame)
    
    mock_df_source.select.return_value = mock_df_filtered
    mock_ingestion_port.read_source_data.return_value = mock_df_source

    use_case = IndexDatasetUseCase(
        ingestion_port=mock_ingestion_port, 
        indexing_port=mock_indexing_port
    )
    
    use_case.execute(
        source_table="tabela_origem", 
        spec=mock_spec, 
        id_field="codigo_nascimento"
    )

    mock_indexing_port.ensure_index_with_mapping.assert_called_once_with("nascimentos_test", mock_spec)
    mock_ingestion_port.read_source_data.assert_called_once_with(table_name="tabela_origem")
    mock_df_source.select.assert_called_once_with("codigo_nascimento", "nome_completo")
    mock_indexing_port.index_dataframe.assert_called_once_with(
        df=mock_df_filtered,
        index_name="nascimentos_test",
        id_field="codigo_nascimento"
    )


# =========================================================================
# 2. CASO DE USO: PROPAGAÇÃO DE ERROS DA PORTA DE INGESTÃO
# =========================================================================

def test_index_dataset_use_case_propagates_ingestion_error():
    mock_ingestion_port = MagicMock(spec=DataIngestionPort)
    mock_indexing_port = MagicMock(spec=DataIndexingPort)
    
    # CORREÇÃO: Montagem explícita dos nós de mock aninhados
    mock_spec = MagicMock(spec=DatasetIndexingSpecification)
    mock_index_config = MagicMock()
    mock_index_config.name = "nascimentos_test"
    mock_spec.index_config = mock_index_config

    mock_ingestion_port.read_source_data.side_effect = IOError("FileSystem link broken or file corrupted")

    use_case = IndexDatasetUseCase(
        ingestion_port=mock_ingestion_port, 
        indexing_port=mock_indexing_port
    )

    with pytest.raises(IOError) as exc_info:
        use_case.execute("tabela_origem", mock_spec, "codigo_nascimento")
        
    assert "FileSystem link broken" in str(exc_info.value)
    mock_indexing_port.index_dataframe.assert_not_called()