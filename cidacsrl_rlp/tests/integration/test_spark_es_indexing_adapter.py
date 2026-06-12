import pytest
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_indexing_adapter import SparkESIndexingAdapter

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("cidacsrl-test-es-indexing")
        .config(
            "spark.jars.packages",
            "org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8"
        )
        .getOrCreate()
    )
    yield spark

@pytest.fixture(scope="session")
def elasticsearch_service(es_url):
    client = Elasticsearch(es_url)
    yield {"es_connection_url": es_url}, client
    client.close()

@pytest.fixture
def spec():
    class IndexConfig:
        name = "test-index"
        number_of_shards = 1
        number_of_replicas = 0
        refresh_interval = "1s"
        id_from_source = False

    class SourceConfig:
        id_field = "id"

    class Column:
        def __init__(self, name, type_, index_as=""):
            self.name = name
            self.type = type_
            self.index_as = index_as

    class Spec:
        index_columns = [
            Column("field1", "text", "both"),
            Column("field2", "keyword"),
        ]
        index_config = IndexConfig()
        source_config = SourceConfig()

    return Spec()

@pytest.fixture(autouse=True)
def cleanup_indices(elasticsearch_service, spec):
    es_config, es_client = elasticsearch_service
    index_name = spec.index_config.name
    if es_client.indices.exists(index=index_name):
        es_client.indices.delete(index=index_name)
    yield
    if es_client.indices.exists(index=index_name):
        es_client.indices.delete(index=index_name)

def test_ensure_index_with_mapping_integration(elasticsearch_service, spec):
    es_config, es_client = elasticsearch_service
    adapter = SparkESIndexingAdapter(es_config)
    adapter.ensure_index_with_mapping(spec)
    assert es_client.indices.exists(index=spec.index_config.name)

def test_index_dataframe_integration(spark, elasticsearch_service, spec):
    es_config, es_client = elasticsearch_service
    adapter = SparkESIndexingAdapter(es_config)
    adapter.ensure_index_with_mapping(spec)
    df = spark.createDataFrame(
        [{"field1": "foo", "field2": "bar"}, {"field1": "baz", "field2": "qux"}]
    )
    adapter.index_dataframe(df, spec)
    es_client.indices.refresh(index=spec.index_config.name)
    res = es_client.search(index=spec.index_config.name, query={"match_all": {}})
    hits = res["hits"]["hits"]
    assert len(hits) == 2
    values = {hit["_source"]["field1"] for hit in hits}
    assert "foo" in values and "baz" in values

def test_index_dataframe_with_id_field(spark, elasticsearch_service):
    class IndexConfig:
        name = "test-index-id"
        number_of_shards = 1
        number_of_replicas = 0
        refresh_interval = "1s"
        id_from_source = True

    class SourceConfig:
        id_field = "id"

    class Column:
        def __init__(self, name, type_, index_as=""):
            self.name = name
            self.type = type_
            self.index_as = index_as

    class Spec:
        index_columns = [
            Column("id", "keyword"),
            Column("field1", "text", "both"),
        ]
        index_config = IndexConfig()
        source_config = SourceConfig()

    spec = Spec()
    es_config, es_client = elasticsearch_service

    if es_client.indices.exists(index=spec.index_config.name):
        es_client.indices.delete(index=spec.index_config.name)

    adapter = SparkESIndexingAdapter(es_config)
    adapter.ensure_index_with_mapping(spec)
    df = spark.createDataFrame(
        [{"id": "1", "field1": "foo"}, {"id": "2", "field1": "bar"}]
    )
    adapter.index_dataframe(df, spec)
    es_client.indices.refresh(index=spec.index_config.name)
    res = es_client.search(index=spec.index_config.name, query={"match_all": {}})
    hits = res["hits"]["hits"]
    assert len(hits) == 2
    ids = {hit["_id"] for hit in hits}
    assert "1" in ids and "2" in ids

    if es_client.indices.exists(index=spec.index_config.name):
        es_client.indices.delete(index=spec.index_config.name)