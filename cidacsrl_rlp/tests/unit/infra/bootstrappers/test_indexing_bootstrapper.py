import pytest
from unittest.mock import patch, MagicMock

from cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper import bootstrap_elasticsearch_indexing
from cidacsrl_rlp.cidacsrl.domain.models.indexing_specification import DatasetIndexingSpecification


# =========================================================================
# 1. CASO DE USO: FLUXO DE SUCESSO (HAPPY PATH)
# =========================================================================

@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.load_linkage_env_config")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.load_es_config")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.load_dataset_indexing_specification")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.SparkDataRepositoryAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.SparkESIndexingAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.IndexDatasetUseCase")
def test_bootstrap_elasticsearch_indexing_success(
    mock_use_case_cls,
    mock_es_indexing_adapter_cls,
    mock_spark_adapter_cls,
    mock_load_spec,
    mock_load_es,
    mock_load_env,
):
    mock_env_config = MagicMock()
    mock_es_config = {"host": "localhost", "port": 9200}
    
    mock_spec = MagicMock(spec=DatasetIndexingSpecification)
    mock_spec.index_config = MagicMock()
    mock_spec.index_config.name = "nascimentos_test"
    
    mock_load_env.return_value = mock_env_config
    mock_load_es.return_value = mock_es_config
    mock_load_spec.return_value = mock_spec
    
    mock_spark_adapter = MagicMock()
    mock_es_indexing_adapter = MagicMock()
    mock_spark_adapter_cls.return_value = mock_spark_adapter
    mock_es_indexing_adapter_cls.return_value = mock_es_indexing_adapter
    
    mock_use_case_instance = MagicMock()
    mock_use_case_cls.return_value = mock_use_case_instance
    
    mock_spark_session = MagicMock()

    bootstrap_elasticsearch_indexing(
        config_path="env.yml",
        indexing_spec_path="indexing.yml",
        spark_session=mock_spark_session
    )

    mock_load_env.assert_called_once_with("env.yml")
    mock_load_es.assert_called_once_with(mock_env_config.es_config_path)
    mock_load_spec.assert_called_once_with("indexing.yml")

    mock_spark_adapter_cls.assert_called_once_with(spark_session=mock_spark_session, env_config=mock_env_config)
    mock_es_indexing_adapter_cls.assert_called_once_with(es_config=mock_es_config)

    mock_use_case_cls.assert_called_once_with(
        ingestion_port=mock_spark_adapter,
        indexing_port=mock_es_indexing_adapter
    )
    
    mock_use_case_instance.execute.assert_called_once_with(
        source_table="nascimentos_test",
        spec=mock_spec,
        id_field="codigo_nascimento"
    )


# =========================================================================
# 2. CASOS DE USO: FALHAS DE CONFIGURAÇÃO (FAIL-FAST)
# =========================================================================

@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.load_linkage_env_config", side_effect=ValueError("Invalid env configuration"))
def test_bootstrap_indexing_fail_fast_env_config(mock_load_env):
    mock_spark_session = MagicMock()
    
    with pytest.raises(ValueError) as exc_info:
        bootstrap_elasticsearch_indexing("invalid_env.yml", "indexing.yml", mock_spark_session)
        
    assert "Invalid env configuration" in str(exc_info.value)


@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.load_linkage_env_config")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.load_es_config", side_effect=FileNotFoundError("ES config file not found"))
def test_bootstrap_indexing_fail_fast_es_config(mock_load_es, mock_load_env):
    mock_env_config = MagicMock()
    mock_env_config.es_config_path = "missing_es.yml"
    mock_load_env.return_value = mock_env_config
    mock_spark_session = MagicMock()
    
    with pytest.raises(FileNotFoundError) as exc_info:
        bootstrap_elasticsearch_indexing("env.yml", "indexing.yml", mock_spark_session)
        
    assert "ES config file not found" in str(exc_info.value)


@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.load_linkage_env_config")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.load_es_config")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.load_dataset_indexing_specification", side_effect=ValueError("Missing columns node"))
def test_bootstrap_indexing_fail_fast_invalid_spec(mock_load_spec, mock_load_es, mock_load_env):
    mock_load_env.return_value = MagicMock()
    mock_load_es.return_value = {}
    mock_spark_session = MagicMock()
    
    with pytest.raises(ValueError) as exc_info:
        bootstrap_elasticsearch_indexing("env.yml", "invalid_spec.yml", mock_spark_session)
        
    assert "Missing columns node" in str(exc_info.value)


# =========================================================================
# 3. CASOS DE USO: FALHAS EM TEMPO DE EXECUÇÃO (RUNTIME ERRORS)
# =========================================================================

@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.load_linkage_env_config")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.load_es_config")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.load_dataset_indexing_specification")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.SparkDataRepositoryAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.SparkESIndexingAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.IndexDatasetUseCase")
def test_bootstrap_indexing_propagates_runtime_exception(
    mock_use_case_cls,
    mock_es_indexing_adapter_cls,
    mock_spark_adapter_cls,
    mock_load_spec,
    mock_load_es,
    mock_load_env,
):
    mock_env_config = MagicMock()
    mock_es_config = {}
    
    # CORREÇÃO: Cria e fia a estrutura interna de mocks aninhados corretamente
    mock_spec = MagicMock(spec=DatasetIndexingSpecification)
    mock_index_config = MagicMock()
    mock_index_config.name = "nascimentos_test"
    mock_spec.index_config = mock_index_config
    
    mock_load_env.return_value = mock_env_config
    mock_load_es.return_value = mock_es_config
    mock_load_spec.return_value = mock_spec
    
    mock_spark_adapter = MagicMock()
    mock_es_indexing_adapter = MagicMock()
    mock_spark_adapter_cls.return_value = mock_spark_adapter
    mock_es_indexing_adapter_cls.return_value = mock_es_indexing_adapter
    
    # Configura o efeito colateral (side_effect) de erro em runtime no execute
    mock_use_case_instance = MagicMock()
    mock_use_case_instance.execute.side_effect = RuntimeError("Spark connection refused or cluster dead")
    mock_use_case_cls.return_value = mock_use_case_instance
    
    mock_spark_session = MagicMock()

    with pytest.raises(RuntimeError) as exc_info:
        bootstrap_elasticsearch_indexing(
            config_path="env.yml", 
            indexing_spec_path="indexing.yml", 
            spark_session=mock_spark_session
        )
        
    assert "Spark connection refused" in str(exc_info.value)