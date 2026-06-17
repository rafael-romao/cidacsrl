import os
import pytest
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, Row

from core.infra.adapters.outbound.elasticsearch.spark_es_search_adapter import SparkESSearchAdapter
from core.infra.adapters.outbound.elasticsearch.executors import MultiSearchExecutor
from core.domain.models.matching_rules import ComparisonRule
from core.domain.models.linkage_specification import BlockingPhaseContext, BlockingPhaseTargetFields


@pytest.fixture(scope="module")
def es_container():
    yield os.environ.get("CIDACSRL_ES_URL", "http://localhost:9200")


@pytest.fixture(scope="module")
def es_client(es_container):
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

    yield {"url": es_container, "index": index_name}

    client.options(ignore_status=[400, 404]).indices.delete(index=index_name)


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("cidacsrl-test-es-search")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark


@pytest.fixture
def real_phase_context():
    rules = [
        ComparisonRule(source_column="nome", target_column="nome_completo", similarity="jaro_winkler", weight=1.0, es_clause_type="must"),
        ComparisonRule(source_column="data_nasc", target_column="data_nascimento", similarity="exact", weight=1.0, es_clause_type="must")
    ]
    
    target_config = BlockingPhaseTargetFields(
        comparison_fields=["nome_completo", "data_nascimento"],
        required_fields=["id_nacional"],
        extra_fields=["sexo"]
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
        index_name=es_client["index"],
        search_executor=MultiSearchExecutor()
    )


def test_get_candidates_successfully_retrieves_records(spark, search_adapter, real_phase_context):
    df_source = spark.createDataFrame([
        Row(id_origem="A1", nome="João Silva", data_nasc="1990-01-01")
    ])

    df_candidates = search_adapter.get_candidates(df_source, real_phase_context)
    results = df_candidates.collect()

    assert len(results) > 0, "Deveria ter encontrado candidatos no Elasticsearch."

    primeiro_match = results[0]
    assert hasattr(primeiro_match, "source_record")
    assert hasattr(primeiro_match, "candidate_record")
    assert primeiro_match.source_record.id_origem == "A1"
    assert primeiro_match.source_record.nome == "João Silva"
    assert primeiro_match.candidate_record.id_nacional == "1"
    assert primeiro_match.candidate_record.nome_completo == "João Silva"
    assert primeiro_match.candidate_record.data_nascimento == "1990-01-01"
    assert primeiro_match.candidate_record.sexo == "M"


def test_get_candidates_returns_empty_if_no_match(spark, search_adapter, real_phase_context):
    df_source = spark.createDataFrame([
        Row(id_origem="B2", nome="Nome Inexistente na Base", data_nasc="2020-01-01")
    ])
    
    df_candidates = search_adapter.get_candidates(df_source, real_phase_context)
    results = df_candidates.collect()
    
    assert len(results) == 0, "Não deveria retornar candidatos para um nome inexistente."


def test_get_candidates_respects_candidate_limit(spark, search_adapter, real_phase_context):
    real_phase_context.candidate_limit = 1

    df_source = spark.createDataFrame([
        Row(id_origem="C3", nome="João", data_nasc="1990-01-01")
    ])

    df_candidates = search_adapter.get_candidates(df_source, real_phase_context)
    results = df_candidates.collect()

    assert len(results) == 1