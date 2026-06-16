from unittest.mock import ANY, MagicMock, patch

import pytest

from core.domain.models.indexing_specification import DatasetIndexingSpecification
from core.infra.bootstrappers.indexing_bootstrapper import bootstrap_elasticsearch_indexing


@pytest.fixture
def storage_config_data():
    return {
        "source_path": "/mock/source",
        "source_format": "parquet",
    }


@pytest.fixture
def indexing_spec_data():
    return {
        "source_config": {
            "source_table": "nascimentos_example",
            "id_field": "codigo_nascimento",
        },
        "index_config": {
            "name": "nascimentos_example_index",
            "id_from_source": True,
        },
        "index_columns": [
            {"name": "codigo_nascimento", "type": "keyword"},
            {"name": "nome_completo", "type": "text", "index_as": "both"},
        ],
    }


@pytest.fixture
def es_config_data():
    return {"es_connection_url": "http://localhost:9200"}


@pytest.fixture
def spark_config_data():
    return {"spark.master": "local[1]"}


@pytest.fixture
def parsed_spec():
    spec = MagicMock(spec=DatasetIndexingSpecification)
    source_cfg = MagicMock()
    source_cfg.source_table = "nascimentos_example"
    index_cfg = MagicMock()
    index_cfg.name = "nascimentos_example_index"
    spec.source_config = source_cfg
    spec.index_config = index_cfg
    return spec


@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.parse_dataset_indexing_specification")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.parse_es_config")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.create_spark_session")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.SparkDataIngestionAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.SparkESIndexingAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.IndexDatasetUseCase")
def test_bootstrap_elasticsearch_indexing_success(
    mock_use_case_cls,
    mock_indexing_adapter_cls,
    mock_ingestion_adapter_cls,
    mock_create_spark_session,
    mock_parse_es,
    mock_parse_spec,
    storage_config_data,
    indexing_spec_data,
    es_config_data,
    spark_config_data,
    parsed_spec,
):
    mock_spark_session = MagicMock()
    mock_create_spark_session.return_value = mock_spark_session
    mock_parse_es.return_value = {"es_connection_url": "http://localhost:9200"}
    mock_parse_spec.return_value = parsed_spec

    mock_ingestion_adapter = MagicMock()
    mock_indexing_adapter = MagicMock()
    mock_ingestion_adapter_cls.return_value = mock_ingestion_adapter
    mock_indexing_adapter_cls.return_value = mock_indexing_adapter

    mock_use_case_instance = MagicMock()
    mock_use_case_cls.return_value = mock_use_case_instance

    bootstrap_elasticsearch_indexing(
        storage_config_data=storage_config_data,
        indexing_spec_data=indexing_spec_data,
        es_config_data=es_config_data,
        spark_config_data=spark_config_data,
    )

    mock_parse_es.assert_called_once_with(es_config_data)
    mock_parse_spec.assert_called_once_with(indexing_spec_data)
    mock_create_spark_session.assert_called_once()
    assert "nascimentos_example" in mock_create_spark_session.call_args.kwargs["app_name"]
    assert "nascimentos_example_index" in mock_create_spark_session.call_args.kwargs["app_name"]
    mock_ingestion_adapter_cls.assert_called_once_with(spark_session=mock_spark_session, config=ANY)
    mock_indexing_adapter_cls.assert_called_once_with(es_config={"es_connection_url": "http://localhost:9200"})
    mock_use_case_cls.assert_called_once_with(
        ingestion_port=mock_ingestion_adapter,
        indexing_port=mock_indexing_adapter,
    )
    mock_use_case_instance.execute.assert_called_once_with(spec=parsed_spec)
    mock_spark_session.stop.assert_called_once()


@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.parse_dataset_indexing_specification")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.parse_es_config")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.create_spark_session")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.SparkDataIngestionAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.SparkESIndexingAdapter")
@patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper.IndexDatasetUseCase")
def test_bootstrap_elasticsearch_indexing_stops_spark_on_error(
    mock_use_case_cls,
    mock_indexing_adapter_cls,
    mock_ingestion_adapter_cls,
    mock_create_spark_session,
    mock_parse_es,
    mock_parse_spec,
    storage_config_data,
    indexing_spec_data,
    es_config_data,
    spark_config_data,
    parsed_spec,
):
    mock_spark_session = MagicMock()
    mock_create_spark_session.return_value = mock_spark_session
    mock_parse_es.return_value = {"es_connection_url": "http://localhost:9200"}
    mock_parse_spec.return_value = parsed_spec
    mock_ingestion_adapter_cls.return_value = MagicMock()
    mock_indexing_adapter_cls.return_value = MagicMock()

    mock_use_case_instance = MagicMock()
    mock_use_case_instance.execute.side_effect = RuntimeError("indexing failed")
    mock_use_case_cls.return_value = mock_use_case_instance

    with pytest.raises(RuntimeError, match="indexing failed"):
        bootstrap_elasticsearch_indexing(
            storage_config_data=storage_config_data,
            indexing_spec_data=indexing_spec_data,
            es_config_data=es_config_data,
            spark_config_data=spark_config_data,
        )

    mock_spark_session.stop.assert_called_once()