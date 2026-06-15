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
    
    # Montagem do mock da especificação técnica
    mock_spec = MagicMock(spec=DatasetIndexingSpecification)
    
    # Simula a estrutura interna de spec.source_config.source_table
    mock_source_config = MagicMock()
    mock_source_config.source_table = "tabela_origem"
    mock_spec.source_config = mock_source_config
    
    # Colunas que serão selecionadas para indexação
    mock_spec.index_columns = [
        IndexColumnConfig(name="codigo_nascimento", type="keyword"),
        IndexColumnConfig(name="nome_completo", type="text")
    ]
    
    mock_df_source = MagicMock(spec=DataFrame)
    mock_df_filtered = MagicMock(spec=DataFrame)
    
    mock_df_source.select.return_value = mock_df_filtered
    
    # Configura a porta para responder ao método correto (read_all)
    mock_ingestion_port.read_all.return_value = mock_df_source

    use_case = IndexDatasetUseCase(
        ingestion_port=mock_ingestion_port, 
        indexing_port=mock_indexing_port
    )
    
    # Correção: Passando estritamente os parâmetros esperados pelo execute() de produção
    use_case.execute(spec=mock_spec)

    # Asserções de comportamento e contratos
    mock_indexing_port.ensure_index_with_mapping.assert_called_once_with(mock_spec)
    mock_ingestion_port.read_all.assert_called_once_with(table_name="tabela_origem")
    mock_df_source.select.assert_called_once_with("codigo_nascimento", "nome_completo")
    mock_indexing_port.index_dataframe.assert_called_once_with(
        df=mock_df_filtered,
        spec=mock_spec
    )


# =========================================================================
# 2. CASO DE USO: PROPAGAÇÃO DE ERROS DA PORTA DE INGESTÃO
# =========================================================================

def test_index_dataset_use_case_propagates_ingestion_error():
    mock_ingestion_port = MagicMock(spec=DataIngestionPort)
    mock_indexing_port = MagicMock(spec=DataIndexingPort)
    
    mock_spec = MagicMock(spec=DatasetIndexingSpecification)
    mock_source_config = MagicMock()
    mock_source_config.source_table = "tabela_origem"
    mock_spec.source_config = mock_source_config

    # Simula uma falha física de I/O na leitura da tabela inteira
    mock_ingestion_port.read_all.side_effect = IOError("FileSystem link broken or file corrupted")

    use_case = IndexDatasetUseCase(
        ingestion_port=mock_ingestion_port, 
        indexing_port=mock_indexing_port
    )

    with pytest.raises(IOError) as exc_info:
        use_case.execute(spec=mock_spec)
        
    assert "FileSystem link broken" in str(exc_info.value)
    mock_indexing_port.index_dataframe.assert_not_called()