import pytest
from testcontainers.elasticsearch import ElasticSearchContainer
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, Row

from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_search_adapter import SparkESSearchAdapter
from cidacsrl_rlp.cidacsrl.domain.models.matching_rules import ComparisonRule
from cidacsrl_rlp.cidacsrl.domain.models.linkage_specification import BlockingPhaseContext, BlockingPhaseTargetFields


# ==========================================
# FIXTURES (Elasticsearch Real via Docker)
# ==========================================

@pytest.fixture(scope="module")
def es_container():
    """
    Sobe um Elasticsearch 9.x real dentro de um contentor Docker.
    Garante que o ES está pronto antes de libertar a execução e o destrói no fim.
    """
    with ElasticSearchContainer("elasticsearch:9.1.8") as es:
        host = es.get_container_host_ip()
        port = es.get_exposed_port(9200)
        yield f"http://{host}:{port}"


@pytest.fixture(scope="module")
def es_client(es_container):
    """Popula o Elasticsearch com dados de teste para os workers procurarem."""
    client = Elasticsearch(es_container)
    index_name = "pacientes_teste"
    
    client.options(ignore_status=400).indices.create(index=index_name)
    
    docs = [
        {"id_nacional": "1", "nome_completo": "João Silva", "data_nascimento": "1990-01-01", "sexo": "M"},
        {"id_nacional": "2", "nome_completo": "Maria Oliveira", "data_nascimento": "1985-05-10", "sexo": "F"},
        {"id_nacional": "3", "nome_completo": "Joãozinho da Silva", "data_nascimento": "1990-01-01", "sexo": "M"}
    ]
    
    for doc in docs:
        client.index(index=index_name, id=doc["id_nacional"], document=doc)
        
    client.indices.refresh(index=index_name)
    
    return {"url": es_container, "index": index_name}


# ==========================================
# FIXTURES (Spark e Domínio)
# ==========================================

@pytest.fixture(scope="module")
def spark():
    """
    Usamos local[2] para simular a distribuição em dois workers/threads,
    garantindo que o código é resiliente à serialização.
    """
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("SparkESSearchAdapterTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def real_phase_context():
    """Contexto rigoroso do domínio que o adaptador terá de respeitar."""
    rules = [
        ComparisonRule(source_column="nome", target_column="nome_completo", similarity="jaro_winkler", weight=1.0),
        ComparisonRule(source_column="data_nasc", target_column="data_nascimento", similarity="exact", weight=1.0)
    ]
    
    target_config = BlockingPhaseTargetFields(
        comparison_fields=["nome_completo", "data_nascimento"],
        required_fields=["id_nacional"],
        extra_fields=["sexo"] # Campo extra que testámos nas refatorações anteriores!
    )
    
    return BlockingPhaseContext(
        phase_name="fase_teste_es",
        rules=rules,
        strong_match_score_threshold=0.8,
        candidate_limit=5,
        target_fields=target_config
    )


@pytest.fixture
def search_adapter(es_client):
    config_dict = {"es_connection_url": es_client["url"]}
    return SparkESSearchAdapter(
        es_config=config_dict,
        index_name=es_client["index"]
        )


# ==========================================
# TESTES DE INTEGRAÇÃO (Workers vs Elasticsearch)
# ==========================================

def test_get_candidates_successfully_retrieves_records(spark, search_adapter, real_phase_context):
    """
    Cenário 1: O worker deve conectar ao ES, enviar a query e trazer o candidato.
    """
    # 1. Preparação: Um dataframe de origem a procurar o "João"
    df_source = spark.createDataFrame([
        Row(id_origem="A1", nome="João Silva", data_nasc="1990-01-01")
    ])
    
    # 2. Execução (A magia acontece nos workers do Spark quando chamamos collect)
    df_candidates = search_adapter.get_candidates(df_source, real_phase_context)
    results = df_candidates.collect()
    
    # 3. Validações
    assert len(results) > 0, "Deveria ter encontrado candidatos no Elasticsearch."
    
    primeiro_match = results[0]
    
    # Valida se a raiz está estruturada corretamente
    assert hasattr(primeiro_match, "source_record")
    assert hasattr(primeiro_match, "candidate_record")
    
    # Valida se a fonte foi preservada intata
    assert primeiro_match.source_record.id_origem == "A1"
    assert primeiro_match.source_record.nome == "João Silva"
    
    # Valida se o candidato trouxe todas as `fetch_fields` (IDs, comparação e extras)
    assert primeiro_match.candidate_record.id_nacional == "1"
    assert primeiro_match.candidate_record.nome_completo == "João Silva"
    assert primeiro_match.candidate_record.data_nascimento == "1990-01-01"
    assert primeiro_match.candidate_record.sexo == "M"  # Garante que o extra_field veio na query


def test_get_candidates_returns_empty_if_no_match(spark, search_adapter, real_phase_context):
    """
    Cenário 2: O adapter não deve quebrar se o ES não devolver resultados para a query.
    """
    df_source = spark.createDataFrame([
        Row(id_origem="B2", nome="Nome Inexistente na Base", data_nasc="2020-01-01")
    ])
    
    df_candidates = search_adapter.get_candidates(df_source, real_phase_context)
    results = df_candidates.collect()
    
    assert len(results) == 0, "Não deveria retornar candidatos para um nome inexistente."


def test_get_candidates_respects_candidate_limit(spark, search_adapter, real_phase_context):
    """
    Cenário 3: Verifica se o limite de candidatos (Top-K) é respeitado pelos workers.
    """
    # Modificamos o contexto para trazer apenas 1 candidato
    real_phase_context.candidate_limit = 1
    
    # Criamos um registo ambíguo que baterá na query (vai encontrar os dois 'João')
    df_source = spark.createDataFrame([
        Row(id_origem="C3", nome="João", data_nasc="1990-01-01")
    ])
    
    df_candidates = search_adapter.get_candidates(df_source, real_phase_context)
    results = df_candidates.collect()
    
    # Embora existam dois "João" no ES com a mesma data ("João Silva" e "Joãozinho da Silva"), 
    # o limite forçado era 1.
    assert len(results) == 1