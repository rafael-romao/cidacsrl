from unittest.mock import MagicMock, patch

from cidacsrl_rlp.cidacsrl.infra.bootstrappers.indexing_bootstrapper import bootstrap_elasticsearch_indexing


def test_bootstrap_elasticsearch_indexing_execution(test_paths):
    captured = {}

    storage_config_data = {
        "source_path": str(test_paths["input_data"]),
        "source_format": "parquet",
    }
    indexing_spec_data = {
        "source_config": {
            "source_table": "nascimentos_example",
            "id_field": "codigo_nascimento",
        },
        "index_config": {
            "name": "nascimentos_example_index_integration",
            "id_from_source": True,
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "1s",
        },
        "index_columns": [
            {"name": "codigo_nascimento", "type": "keyword"},
            {"name": "nome_completo", "type": "text", "index_as": "both"},
            {"name": "nome_mae", "type": "text", "index_as": "both"},
        ],
    }
    es_config_data = {
        "es_connection_url": "http://localhost:9200",
    }
    spark_config_data = {
        "spark.master": "local[1]",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "1",
    }

    fake_es_client = MagicMock()
    fake_es_client.indices.exists.return_value = False

    def capture_index_dataframe(_self, df, spec):
        captured["columns"] = df.columns
        captured["row_count"] = df.count()
        captured["index_name"] = spec.index_config.name

    with patch(
        "cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_indexing_adapter.get_es_client",
        return_value=fake_es_client,
    ), patch(
        "cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_indexing_adapter.SparkESIndexingAdapter.index_dataframe",
        autospec=True,
        side_effect=capture_index_dataframe,
    ):
        bootstrap_elasticsearch_indexing(
            storage_config_data=storage_config_data,
            indexing_spec_data=indexing_spec_data,
            es_config_data=es_config_data,
            spark_config_data=spark_config_data,
        )

    fake_es_client.indices.create.assert_called_once()
    assert captured["index_name"] == "nascimentos_example_index_integration"
    assert captured["row_count"] > 0
    assert captured["columns"] == ["codigo_nascimento", "nome_completo", "nome_mae"]