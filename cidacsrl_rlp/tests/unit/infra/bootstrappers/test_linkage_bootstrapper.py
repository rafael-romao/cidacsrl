import pytest
from unittest.mock import ANY, MagicMock, patch

from cidacsrl_rlp.cidacsrl.application.use_cases.run_sequential_linkage import RunSequentialLinkageUseCase
from cidacsrl_rlp.cidacsrl.domain.models.linkage_specification import SequentialLinkageSpecification
from cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper import bootstrap_sequential_linkage


@pytest.fixture
def storage_config_data():
    return {
        "source_data_path": "/mock/source",
        "output_data_path": "/mock/output",
        "source_data_format": "parquet",
        "output_data_format": "parquet",
    }


@pytest.fixture
def linkage_spec_data():
    return {
        "source_table": "internacao_example",
        "id_source_table": "source_id",
        "target_es_index": "nascimentos_example",
        "id_target_table": "candidate_id",
        "blocking_phases": [],
    }


@pytest.fixture
def es_config_data():
    return {"es_connection_url": "http://localhost:9200", "request_timeout": 30, "msearch_batch_size": 100}


@pytest.fixture
def spark_config_data():
    return {"spark.master": "local[1]"}


@pytest.fixture
def parsed_spec():
    spec = MagicMock(spec=SequentialLinkageSpecification)
    spec.source_table = "internacao_example"
    spec.target_es_index = "nascimentos_example"
    return spec


@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.parse_sequential_linkage_specification")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.parse_es_config")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.get_es_client")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.create_spark_session")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataIngestionAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataPersistenceAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataTransformationAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkESSearchAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkScoringAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.RunSequentialLinkageUseCase")
def test_bootstrap_sequential_linkage_success(
    mock_use_case_cls,
    mock_scoring_adapter_cls,
    mock_search_adapter_cls,
    mock_transformation_adapter_cls,
    mock_persistence_adapter_cls,
    mock_ingestion_adapter_cls,
    mock_create_spark_session,
    mock_get_es_client,
    mock_parse_es,
    mock_parse_spec,
    storage_config_data,
    linkage_spec_data,
    es_config_data,
    spark_config_data,
    parsed_spec,
):
    mock_spark_session = MagicMock()
    mock_create_spark_session.return_value = mock_spark_session
    mock_parse_es.return_value = {"es_connection_url": "http://localhost:9200"}
    mock_parse_spec.return_value = parsed_spec
    mock_get_es_client.return_value = MagicMock()

    mock_ingestion_adapter = MagicMock()
    mock_ingestion_adapter.check_health.return_value = []
    mock_persistence_adapter = MagicMock()
    mock_transformation_adapter = MagicMock()
    mock_search_adapter = MagicMock()
    mock_scoring_adapter = MagicMock()

    mock_ingestion_adapter_cls.return_value = mock_ingestion_adapter
    mock_persistence_adapter_cls.return_value = mock_persistence_adapter
    mock_transformation_adapter_cls.return_value = mock_transformation_adapter
    mock_search_adapter_cls.return_value = mock_search_adapter
    mock_scoring_adapter_cls.return_value = mock_scoring_adapter

    mock_use_case_instance = MagicMock()
    mock_use_case_cls.return_value = mock_use_case_instance

    bootstrap_sequential_linkage(
        storage_config_data=storage_config_data,
        linkage_spec_data=linkage_spec_data,
        es_config_data=es_config_data,
        spark_config_data=spark_config_data,
    )

    mock_parse_es.assert_called_once_with(es_config_data)
    mock_parse_spec.assert_called_once_with(linkage_spec_data)
    mock_get_es_client.assert_called_once_with({"es_connection_url": "http://localhost:9200"}, use_cache=False)
    mock_create_spark_session.assert_called_once()
    assert "internacao_example" in mock_create_spark_session.call_args.kwargs["app_name"]
    assert "nascimentos_example" in mock_create_spark_session.call_args.kwargs["app_name"]
    mock_ingestion_adapter_cls.assert_called_once_with(spark_session=mock_spark_session, config=ANY)
    mock_persistence_adapter_cls.assert_called_once_with(spark_session=mock_spark_session, config=ANY)
    mock_transformation_adapter_cls.assert_called_once_with()
    mock_search_adapter_cls.assert_called_once_with(index_name="nascimentos_example", es_config={"es_connection_url": "http://localhost:9200"})
    mock_scoring_adapter_cls.assert_called_once_with()
    mock_use_case_cls.assert_called_once_with(
        ingestion_port=mock_ingestion_adapter,
        persistence_port=mock_persistence_adapter,
        transformation_port=mock_transformation_adapter,
        get_candidates_port=mock_search_adapter,
        scoring_port=mock_scoring_adapter,
    )
    mock_use_case_instance.execute.assert_called_once_with(config=parsed_spec)


@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.parse_sequential_linkage_specification")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.parse_es_config")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.get_es_client")
def test_bootstrap_fails_when_elasticsearch_is_offline(
    mock_get_es_client,
    mock_parse_es,
    mock_parse_spec,
):
    mock_parse_es.return_value = {"es_connection_url": "http://localhost:9200"}
    mock_spec = MagicMock(spec=SequentialLinkageSpecification)
    mock_spec.source_table = "internacao_example"
    mock_spec.target_es_index = "nascimentos_example"
    mock_parse_spec.return_value = mock_spec
    mock_get_es_client.return_value = None

    with pytest.raises(ConnectionError) as exc_info:
        bootstrap_sequential_linkage(
            storage_config_data={"source_data_path": "/mock/source", "output_data_path": "/mock/output"},
            linkage_spec_data={"source_table": "internacao_example", "id_source_table": "source_id", "target_es_index": "nascimentos_example", "id_target_table": "candidate_id", "blocking_phases": []},
            es_config_data={"es_connection_url": "http://localhost:9200"},
            spark_config_data={},
        )

    assert "Falha crítica de conectividade com o Elasticsearch" in str(exc_info.value)


@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.parse_sequential_linkage_specification")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.parse_es_config")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.get_es_client")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.create_spark_session")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataIngestionAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataPersistenceAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkDataTransformationAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkESSearchAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.SparkScoringAdapter")
def test_bootstrap_fails_on_unhealthy_infrastructure(
    mock_scoring_adapter_cls,
    mock_search_adapter_cls,
    mock_transformation_adapter_cls,
    mock_persistence_adapter_cls,
    mock_ingestion_adapter_cls,
    mock_create_spark_session,
    mock_get_es_client,
    mock_parse_es,
    mock_parse_spec,
):
    mock_spark_session = MagicMock()
    mock_create_spark_session.return_value = mock_spark_session
    mock_parse_es.return_value = {"es_connection_url": "http://localhost:9200"}
    mock_spec = MagicMock(spec=SequentialLinkageSpecification)
    mock_spec.source_table = "internacao_example"
    mock_spec.target_es_index = "nascimentos_example"
    mock_parse_spec.return_value = mock_spec
    mock_get_es_client.return_value = MagicMock()

    mock_ingestion_adapter = MagicMock()
    mock_ingestion_adapter.check_health.return_value = ["Falha ao acessar o filesystem para LEITURA"]
    mock_ingestion_adapter_cls.return_value = mock_ingestion_adapter
    mock_persistence_adapter_cls.return_value = MagicMock()
    mock_transformation_adapter_cls.return_value = MagicMock()
    mock_search_adapter_cls.return_value = MagicMock()
    mock_scoring_adapter_cls.return_value = MagicMock()

    with pytest.raises(ValueError) as exc_info:
        bootstrap_sequential_linkage(
            storage_config_data={"source_data_path": "/mock/source", "output_data_path": "/mock/output"},
            linkage_spec_data={"source_table": "internacao_example", "id_source_table": "source_id", "target_es_index": "nascimentos_example", "id_target_table": "candidate_id", "blocking_phases": []},
            es_config_data={"es_connection_url": "http://localhost:9200"},
            spark_config_data={},
        )

    assert "Falha de Infraestrutura:" in str(exc_info.value)
    assert "Falha ao acessar o filesystem para LEITURA" in str(exc_info.value)