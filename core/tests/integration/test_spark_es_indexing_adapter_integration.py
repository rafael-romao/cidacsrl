import os
import pytest
import uuid
from unittest.mock import MagicMock

from elasticsearch import Elasticsearch
from pyspark.sql import Row



from core.infra.adapters.outbound.elasticsearch.spark_es_indexing_adapter import SparkESIndexingAdapter
from core.domain.models.indexing_specification import DatasetIndexingSpecification, IndexColumnConfig



@pytest.fixture(scope="module")
def es_container():
    yield os.environ.get("CIDACSRL_ES_URL", "http://localhost:9200")

@pytest.fixture(scope="module")
def spark():
    import glob
    from pyspark.sql import SparkSession
    from pyspark import SparkContext

    existing = SparkSession.getActiveSession()
    if existing:
        existing.stop()
    if SparkContext._active_spark_context is not None:
        SparkContext._active_spark_context.stop()

    # Usa o JAR já resolvido pelo Ivy para evitar que o getOrCreate de uma sessão
    # reaproveitada ignore o spark.jars.packages.
    ivy_jar = next(
        iter(glob.glob("/root/.ivy2/cache/org.elasticsearch/elasticsearch-spark-30_2.12/jars/elasticsearch-spark-30_2.12-*.jar")),
        ""
    )
    builder = SparkSession.builder \
        .master("local[2]") \
        .appName("cidacsrl-test-es-indexing-integration") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.ui.enabled", "false") \
        .config("spark.port.maxRetries", "100") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8")

    if ivy_jar:
        builder = builder.config("spark.jars", ivy_jar)

    session = builder.getOrCreate()
    yield session
    session.stop()

def test_adapter_ensure_index_and_bulk_ingestion_integration(es_container, spark):
    connection_url = es_container

    es_config = {
        "es_connection_url": connection_url,
        "wan_only": True
    }

    index_name = f"nascimentos_integration_test_{uuid.uuid4().hex[:8]}"

    mock_spec = MagicMock(spec=DatasetIndexingSpecification)
    mock_index_config = MagicMock()
    mock_index_config.name = index_name
    mock_index_config.number_of_shards = 1
    mock_index_config.number_of_replicas = 0
    mock_index_config.refresh_interval = "1s"
    mock_index_config.id_from_source = True
    mock_spec.index_config = mock_index_config
    mock_spec.index_columns = [
        IndexColumnConfig(name="codigo_nascimento", type="keyword"),
        IndexColumnConfig(name="nome_completo", type="text", index_as="both"),
        IndexColumnConfig(name="uf_nascimento", type="keyword")
    ]
    mock_spec.source_config = MagicMock()
    mock_spec.source_config.id_field = "codigo_nascimento"

    adapter = SparkESIndexingAdapter(es_config=es_config)

    try:
        adapter.ensure_index_with_mapping(mock_spec)

        data = [
            Row(codigo_nascimento="111", nome_completo="MARIA DA SILVA", uf_nascimento="BA"),
            Row(codigo_nascimento="222", nome_completo="JOAO DOS SANTOS", uf_nascimento="BA")
        ]
        df_source = spark.createDataFrame(data)

        adapter.index_dataframe(df_source, mock_spec)

        client = Elasticsearch(connection_url)
        client.indices.refresh(index=index_name)

        assert client.indices.exists(index=index_name), "Índice não foi criado no Elasticsearch"

        mapping = client.indices.get_mapping(index=index_name)
        properties = mapping[index_name]["mappings"]["properties"]
        assert properties["codigo_nascimento"]["type"] == "keyword", "Tipo incorreto para codigo_nascimento"
        assert properties["nome_completo"]["type"] == "text", "Tipo incorreto para nome_completo"
        assert "keyword" in properties["nome_completo"]["fields"], "Campo keyword ausente em nome_completo"

        search_result = client.search(index=index_name, body={"query": {"match_all": {}}})
        assert search_result["hits"]["total"]["value"] == 2, "Quantidade de documentos diferente do esperado"

        hits = search_result["hits"]["hits"]
        document_ids = [hit["_id"] for hit in hits]
        assert "111" in document_ids, "ID 111 não encontrado"
        assert "222" in document_ids, "ID 222 não encontrado"
    finally:
        client = Elasticsearch(connection_url)
        if client.indices.exists(index=index_name):
            client.indices.delete(index=index_name)


def test_adapter_ensure_index_idempotency_integration(es_container, spark):
    es_config = {
        "es_connection_url": es_container,
        "wan_only": True
    }
    
    adapter = SparkESIndexingAdapter(es_config=es_config)

    mock_spec = MagicMock(spec=DatasetIndexingSpecification)
    mock_index_config = MagicMock()
    mock_index_config.name = "idempotent_index_test"
    mock_index_config.number_of_shards = 1
    mock_index_config.number_of_replicas = 0
    mock_index_config.refresh_interval = "1s"
    mock_index_config.id_from_source = False
    mock_spec.index_config = mock_index_config
    mock_spec.index_columns = [IndexColumnConfig(name="id", type="keyword")]

    adapter.ensure_index_with_mapping(mock_spec)

    try:
        adapter.ensure_index_with_mapping(mock_spec)
    except Exception as e:
        pytest.fail(f"O metodo ensure_index_with_mapping quebrou na chamada de indice ja existente: {e}")