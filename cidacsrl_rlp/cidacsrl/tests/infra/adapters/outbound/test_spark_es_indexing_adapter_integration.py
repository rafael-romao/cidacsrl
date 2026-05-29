import pytest
from unittest.mock import MagicMock
from testcontainers.elasticsearch import ElasticSearchContainer
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, Row

from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_indexing_adapter import SparkESIndexingAdapter
from cidacsrl_rlp.cidacsrl.domain.models.indexing_specification import DatasetIndexingSpecification, IndexColumnConfig


# =========================================================================
# FIXTURES DE INFRAESTRUTURA EFÊMERA (Testcontainers + Spark)
# =========================================================================

@pytest.fixture(scope="module")
def es_container():
    """Levanta um container real e isolado do Elasticsearch usando a versao 9.1.8."""
    with ElasticSearchContainer("elasticsearch:9.1.8") as es:
        yield es


@pytest.fixture(scope="module")
def spark_session_integration():
    """Inicializa uma sessao local do Spark injetando o JAR do conector do Elasticsearch."""
    spark = SparkSession.builder \
        .appName("CIDACS-RL-Indexing-Integration-Test") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()


# =========================================================================
# CASOS DE USO: TESTES DE INTEGRAÇÃO REAL COM O MOTOR DO ES
# =========================================================================

def test_adapter_ensure_index_and_bulk_ingestion_integration(es_container, spark_session_integration):
    spark = spark_session_integration
    
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
    
    index_name = "nascimentos_integration_test"
    adapter = SparkESIndexingAdapter(es_config=es_config)
    
    adapter.ensure_index_with_mapping(index_name, mock_spec)
    
    data = [
        Row(codigo_nascimento="111", nome_completo="MARIA DA SILVA", uf_nascimento="BA"),
        Row(codigo_nascimento="222", nome_completo="JOAO DOS SANTOS", uf_nascimento="BA")
    ]
    df_source = spark.createDataFrame(data)
    
    adapter.index_dataframe(df_source, index_name, id_field="codigo_nascimento")
    
    client = Elasticsearch(connection_url)
    client.indices.refresh(index=index_name)
    
    assert bool(client.indices.exists(index=index_name))
    
    mapping = client.indices.get_mapping(index=index_name)
    properties = mapping[index_name]["mappings"]["properties"]
    assert properties["codigo_nascimento"]["type"] == "keyword"
    assert properties["nome_completo"]["type"] == "text"
    assert "keyword" in properties["nome_completo"]["fields"]
    
    search_result = client.search(index=index_name, body={"query": {"match_all": {}}})
    assert search_result["hits"]["total"]["value"] == 2
    
    hits = search_result["hits"]["hits"]
    document_ids = [hit["_id"] for hit in hits]
    assert "111" in document_ids
    assert "222" in document_ids


def test_adapter_ensure_index_idempotency_integration(es_container):
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