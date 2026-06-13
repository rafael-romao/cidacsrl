import pytest
from unittest.mock import MagicMock, patch, ANY
from typing import Dict, Any as AnyType

from cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper import bootstrap_sequential_linkage
from cidacsrl_rlp.cidacsrl.infra.configs.models.storage_config import SourceStorageConfig, OutputStorageConfig
from cidacsrl_rlp.cidacsrl.domain.models.linkage_specification import SequentialLinkageSpecification

@pytest.fixture
def mock_env_local_data() -> Dict[str, AnyType]:
    """Simula os blocos extraídos diretamente do novo env_local.yml unificado."""
    return {
        "storage": {
            "source_path": "tests/data/input",
            "source_format": "parquet",
            "output_path": "tests/data/output",
            "output_format": "parquet"
        },
        "execution": {
            "sample_fraction": 0.1,
            "sample_seed": 42,
            "audit_log_path": "tests/data/audit_logs",
            "partitioning": {
                "partition_column": "uf_internacao",
                "filter_partitions": ["BA"]
            }
        },
        "spark": {
            "spark_configs": {
                "spark.master": "local[*]",
                "spark.sql.shuffle.partitions": "2"
            }
        },
        "elasticsearch": {
            "host": "localhost",
            "port": 9200,
            "es_connection_url": "http://localhost:9200",
            "wan_only": True,
            "search_strategy": "multisearch"
        }
    }

@pytest.fixture
def mock_linkage_spec_data() -> Dict[str, AnyType]:
    """Simula o conteúdo do arquivo de especificação abstrata de domínio (linkage_spec_local.yml)."""
    return {
        "source_table": "internacao_example",
        "target_es_index": "nascimentos_example_index",
        "id_source_table": "codigo_internacao",
        "id_target_table": "codigo_nascimento",
        "blocking_phases": [
            {
                "phase_name": "fase_teste_rapido",
                "enabled": True,
                "candidate_limit": 10,
                "strong_match_score_threshold": 0.9,
                "rules": [
                    {
                        "source_column": "nome_completo",
                        "target_column": "nome_completo",
                        "es_clause_type": "must",
                        "similarity": "exact",
                        "weight": 1.0
                    }
                ]
            }
        ]
    }

@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.get_es_client")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.validate_elasticsearch_schema")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.create_spark_session")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataIngestionAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataPersistenceAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkESSearchAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.RunSequentialLinkageUseCase")
def test_bootstrap_sequential_linkage_sucesso_com_multisearch(
    mock_use_case_cls,
    mock_search_adapter_cls,
    mock_persistence_adapter_cls,
    mock_ingestion_adapter_cls,
    mock_create_spark,
    mock_validate_schema,
    mock_get_es_client,
    mock_env_local_data,
    mock_linkage_spec_data
):
    """Garante que o bootstrapper monta o ecossistema corretamente usando a estratégia MultiSearch."""
    mock_spark = MagicMock()
    mock_create_spark.return_value = mock_spark
    
    mock_ingestion = MagicMock()
    mock_ingestion.check_health.return_value = []  
    mock_ingestion_adapter_cls.return_value = mock_ingestion
    
    mock_use_case_instance = MagicMock()
    mock_use_case_cls.return_value = mock_use_case_instance

    # Act
    bootstrap_sequential_linkage(
        storage_config_data=mock_env_local_data["storage"],
        execution_config_data=mock_env_local_data["execution"],
        linkage_spec_data=mock_linkage_spec_data,
        es_config_data=mock_env_local_data["elasticsearch"],
        spark_config_data=mock_env_local_data["spark"]
    )

   
    mock_get_es_client.assert_called_once()
    mock_validate_schema.assert_called_once()

    
    _, kwargs = mock_search_adapter_cls.call_args
    assert kwargs["index_name"] == "nascimentos_example_index"
    assert kwargs["executor"].__class__.__name__ == "MultiSearchExecutor"

   
    mock_ingestion_adapter_cls.assert_called_once_with(spark_session=mock_spark, config=ANY)
    mock_persistence_adapter_cls.assert_called_once_with(spark_session=mock_spark, config=ANY)

    # Assert 4: Execução do Use Case core
    mock_use_case_instance.execute.assert_called_once()


@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.get_es_client")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.validate_elasticsearch_schema")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.create_spark_session")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataIngestionAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataPersistenceAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkESSearchAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.RunSequentialLinkageUseCase")
def test_bootstrap_sequential_linkage_altera_para_single_search(
    mock_use_case_cls,
    mock_search_adapter_cls,
    mock_persistence_adapter_cls,
    mock_ingestion_adapter_cls,
    mock_create_spark,
    mock_validate_schema,
    mock_get_es_client,
    mock_env_local_data,
    mock_linkage_spec_data
):
    """Verifica se o bootstrapper altera dinamicamente a estratégia se 'single' for fornecido."""
    mock_spark = MagicMock()
    mock_create_spark.return_value = mock_spark
    
    mock_ingestion = MagicMock()
    mock_ingestion.check_health.return_value = []
    mock_ingestion_adapter_cls.return_value = mock_ingestion
    
    # Altera a estratégia no dicionário de ambiente antes de rodar o teste
    mock_env_local_data["elasticsearch"]["search_strategy"] = "single"

    # Act
    bootstrap_sequential_linkage(
        storage_config_data=mock_env_local_data["storage"],
        execution_config_data=mock_env_local_data["execution"],
        linkage_spec_data=mock_linkage_spec_data,
        es_config_data=mock_env_local_data["elasticsearch"],
        spark_config_data=mock_env_local_data["spark"]
    )

    # Assert: O adaptador de busca deve ter recebido o SingleSearchExecutor
    _, kwargs = mock_search_adapter_cls.call_args
    assert kwargs["executor"].__class__.__name__ == "SingleSearchExecutor"


@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.get_es_client")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.validate_elasticsearch_schema")
def test_bootstrap_sequential_linkage_falha_estrategia_invalida(
    mock_validate_schema,
    mock_get_es_client,
    mock_env_local_data,
    mock_linkage_spec_data
):
    """Garante que um erro de valor é lançado se uma estratégia desconhecida for configurada."""
    mock_env_local_data["elasticsearch"]["search_strategy"] = "invalid_strategy_name"

    with pytest.raises(ValueError, match="Estratégia de busca desconhecida: 'invalid_strategy_name'"):
        bootstrap_sequential_linkage(
            storage_config_data=mock_env_local_data["storage"],
            execution_config_data=mock_env_local_data["execution"],
            linkage_spec_data=mock_linkage_spec_data,
            es_config_data=mock_env_local_data["elasticsearch"],
            spark_config_data=mock_env_local_data["spark"]
        )