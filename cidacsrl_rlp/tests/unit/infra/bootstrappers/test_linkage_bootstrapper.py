import pytest
from unittest.mock import MagicMock, patch
from cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper import bootstrap_sequential_linkage


@pytest.fixture
def mock_spark_session():
    """Gera uma instância mockada do SparkSession."""
    return MagicMock()


@pytest.fixture
def mock_configs():
    """Gera instâncias mockadas das configurações carregadas via YAML baseadas no novo design."""
    env_config = MagicMock()
    env_config.linkage_specification_path = "/mock/path/linkage_specification.yml"
    env_config.es_config_path = "/mock/path/elasticsearch_config.yml"
    env_config.source_data_path = "/mock/path/source"
    env_config.output_data_path = "/mock/path/output"
    env_config.source_data_format = "parquet"
    env_config.output_data_format = "parquet"
    
    linkage_specification = MagicMock()
    linkage_specification.source_table = "internacao_example"
    linkage_specification.target_es_index = "nascimentos_example"
    
    es_config = MagicMock()
    
    return env_config, linkage_specification, es_config


class TestLinkageBootstrapper:
    
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.load_linkage_env_config")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.load_sequential_linkage_specification")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.load_es_config")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.get_es_client")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataRepositoryAdapter")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkESSearchAdapter")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkScoringAdapter")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.RunSequentialLinkageUseCase")
    def test_bootstrap_sequential_linkage_success(
        self,
        mock_use_case_cls,
        mock_scoring_adapter_cls,
        mock_search_adapter_cls,
        mock_repo_adapter_cls,
        mock_get_es_client,
        mock_load_es,
        mock_load_spec,
        mock_load_env,
        mock_spark_session,
        mock_configs
    ):
        """Caminho Feliz: Garante que todos os componentes são instanciados e injetados corretamente."""
        
        # 1. Configura o comportamento dos mocks de leitura de arquivos
        env_config, linkage_specification, es_config = mock_configs
        mock_load_env.return_value = env_config
        mock_load_spec.return_value = linkage_specification
        mock_load_es.return_value = es_config

        # 2. Configura o mock do client Elasticsearch para simular sucesso na conexão
        mock_get_es_client.return_value = MagicMock()

        # 3. Configura o mock do SparkDataRepositoryAdapter para passar no sanity check (lista de erros vazia)
        mock_repo_instance = MagicMock()
        mock_repo_instance.check_health.return_value = []
        mock_repo_adapter_cls.return_value = mock_repo_instance

        # 4. Configura o mock do Use Case
        mock_use_case_instance = MagicMock()
        mock_use_case_cls.return_value = mock_use_case_instance

        # --- Execução ---
        bootstrap_sequential_linkage(config_path="/fake/path.yml", spark_session=mock_spark_session)

        # --- Verificações (Asserts) ---
        mock_load_env.assert_called_once_with("/fake/path.yml")
        mock_load_spec.assert_called_once_with(env_config.linkage_specification_path)
        mock_load_es.assert_called_once_with(env_config.es_config_path)
        mock_get_es_client.assert_called_once_with(es_config, use_cache=False)
        mock_repo_adapter_cls.assert_called_once_with(spark_session=mock_spark_session, env_config=env_config)

        mock_repo_instance.check_health.assert_called_once_with(
            linkage_specification.source_table, linkage_specification.target_es_index
        )

        mock_use_case_cls.assert_called_once_with(
            ingestion_port=mock_repo_instance,
            persistence_port=mock_repo_instance,
            transformation_port=mock_repo_instance,
            get_candidates_port=mock_search_adapter_cls.return_value,
            scoring_port=mock_scoring_adapter_cls.return_value
        )

        mock_use_case_instance.execute.assert_called_once_with(linkage_specification)

    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.load_linkage_env_config")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.load_sequential_linkage_specification")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.load_es_config")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.get_es_client")
    def test_bootstrap_fails_when_elasticsearch_is_offline(
        self,
        mock_get_es_client,
        mock_load_es,
        mock_load_spec,
        mock_load_env,
        mock_spark_session,
        mock_configs
    ):
        """Caminho de Falha: Força erro de conectividade preventiva com o Elasticsearch."""
        env_config, linkage_specification, es_config = mock_configs
        mock_load_env.return_value = env_config
        mock_load_spec.return_value = linkage_specification
        mock_load_es.return_value = es_config

        mock_get_es_client.return_value = None

        with pytest.raises(ConnectionError) as exc_info:
            bootstrap_sequential_linkage(config_path="/fake/path.yml", spark_session=mock_spark_session)

        assert "Falha crítica de conectividade com o Elasticsearch" in str(exc_info.value)

    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.load_linkage_env_config")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.load_sequential_linkage_specification")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.load_es_config")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.get_es_client")
    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataRepositoryAdapter")
    def test_bootstrap_fails_on_unhealthy_infrastructure(
        self,
        mock_repo_adapter_cls,
        mock_get_es_client,
        mock_load_es,
        mock_load_spec,
        mock_load_env,
        mock_spark_session,
        mock_configs
    ):
        """Caminho de Falha: Força o erro de diagnóstico (check_health) no FileSystem do Spark."""
        env_config, linkage_specification, es_config = mock_configs
        mock_load_env.return_value = env_config
        mock_load_spec.return_value = linkage_specification
        mock_load_es.return_value = es_config
        mock_get_es_client.return_value = MagicMock()

        mock_repo_instance = MagicMock()
        mock_repo_instance.check_health.return_value = ["Falha ao acessar o filesystem para LEITURA"]
        mock_repo_adapter_cls.return_value = mock_repo_instance

        with pytest.raises(ValueError) as exc_info:
            bootstrap_sequential_linkage(config_path="/fake/path.yml", spark_session=mock_spark_session)

        assert "Falha de Infraestrutura:" in str(exc_info.value)
        assert "Falha ao acessar o filesystem para LEITURA" in str(exc_info.value)