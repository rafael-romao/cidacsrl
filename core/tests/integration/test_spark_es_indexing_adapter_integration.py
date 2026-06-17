import pytest
import uuid
from unittest.mock import MagicMock

from testcontainers.elasticsearch import ElasticSearchContainer
from elasticsearch import Elasticsearch
from pyspark.sql import Row



from core.infra.adapters.outbound.elasticsearch.spark_es_indexing_adapter import SparkESIndexingAdapter
from core.domain.models.indexing_specification import DatasetIndexingSpecification, IndexColumnConfig



@pytest.fixture(scope="module")
def es_container():
    with ElasticSearchContainer("elasticsearch:9.1.8") as es:
        yield es

@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder \
        .master("local[2]") \
        .appName("cidacsrl-test-es-indexing-integration") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.ui.enabled", "false") \
        .config("spark.port.maxRetries", "100") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8") \
        .getOrCreate()

    yield session

def test_adapter_ensure_index_and_bulk_ingestion_integration(es_container, spark):
    host = es_container.get_container_host_ip()
    port = es_container.get_exposed_port(9200)
    connection_url = f"http://{host}:{port}"

    es_config = {
        "host": host,
        "port": int(port),
        "es_connection_url": connection_url,
        "wan_only": True
    }

    mock_spec = MagicMock(spec=DatasetIndexingSpecification)
    mock_index_config = MagicMock()
    mock_index_config.number_of_shards = 1
    mock_index_config.number_of_replicas = 0
    mock_index_config.refresh_interval = "1s"
    mock_spec.index_config = mock_index_config

    mock_spec.columns = [
        IndexColumnConfig(name="codigo_nascimento", type="keyword"),
        IndexColumnConfig(name="nome_completo", type="text", index_as="both"),
        IndexColumnConfig(name="uf_nascimento", type="keyword")
    ]

    index_name = f"nascimentos_integration_test_{uuid.uuid4().hex[:8]}"
    adapter = SparkESIndexingAdapter(es_config=es_config)

    try:
        adapter.ensure_index_with_mapping(index_name, mock_spec)

        data = [
            Row(codigo_nascimento="111", nome_completo="MARIA DA SILVA", uf_nascimento="BA"),
            Row(codigo_nascimento="222", nome_completo="JOAO DOS SANTOS", uf_nascimento="BA")
        ]
        df_source = spark.createDataFrame(data)

        adapter.index_dataframe(df_source, index_name, id_field="codigo_nascimento")

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
    host = es_container.get_container_host_ip()
    port = es_container.get_exposed_port(9200)
    
    es_config = {
        "host": host,
        "port": int(port),
        "es_connection_url": f"http://{host}:{port}",
        "wan_only": True
    }
    
    adapter = SparkESIndexingAdapter(es_config=es_config)

    mock_spec = MagicMock(spec=DatasetIndexingSpecification)
    mock_index_config = MagicMock()
    mock_index_config.number_of_shards = 1
    mock_index_config.number_of_replicas = 0
    mock_index_config.refresh_interval = "1s"
    mock_spec.index_config = mock_index_config

    mock_spec.columns = [IndexColumnConfig(name="id", type="keyword")]

    index_name = "idempotent_index_test"

    adapter.ensure_index_with_mapping(index_name, mock_spec)

    try:
        adapter.ensure_index_with_mapping(index_name, mock_spec)
    except Exception as e:
        pytest.fail(f"O metodo ensure_index_with_mapping quebrou na chamada de indice ja existente: {e}")