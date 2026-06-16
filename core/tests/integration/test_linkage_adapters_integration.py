import pytest
from testcontainers.elasticsearch import ElasticSearchContainer
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, Row

from core.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_search_adapter import SparkESSearchAdapter
from core.cidacsrl.infra.adapters.outbound.elasticsearch.executors import MultiSearchExecutor
from core.cidacsrl.infra.adapters.outbound.spark_scoring_adapter import SparkScoringAdapter
from core.cidacsrl.domain.models.matching_rules import ComparisonRule
from core.cidacsrl.domain.models.linkage_specification import BlockingPhaseContext, BlockingPhaseTargetFields

pytestmark = pytest.mark.integration


@pytest.fixture
def es_container():
    with ElasticSearchContainer("elasticsearch:9.1.8") as es:
        host = es.get_container_host_ip()
        port = es.get_exposed_port(9200)
        yield f"http://{host}:{port}"


@pytest.fixture
def es_client(es_container):
    client = Elasticsearch(es_container)
    index_name = "pacientes_integracao"

    client.options(ignore_status=400).indices.create(index=index_name)

    docs = [
        {"id_nacional": "100", "nome_completo": "Carlos Rocha", "idade": 45, "sexo": "M"},
        {"id_nacional": "200", "nome_completo": "Carla Rocha", "idade": 42, "sexo": "F"}
    ]
    
    for doc in docs:
        client.index(index=index_name, id=doc["id_nacional"], document=doc)
        
    client.indices.refresh(index=index_name)
    return {"url": es_container, "index": index_name}

@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder \
        .appName("cidacsrl-test-linkage-adapters") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()

    yield session


@pytest.fixture
def integration_phase_context():
    rules = [
        ComparisonRule(source_column="nome", target_column="nome_completo", similarity="exact", weight=2.0, es_clause_type="must"),
        ComparisonRule(source_column="idade", target_column="idade", similarity="exact", weight=1.0, es_clause_type="must")
    ]
    
    target_config = BlockingPhaseTargetFields(
        comparison_fields=["nome_completo", "idade"],
        required_fields=["id_nacional"],
        extra_fields=["sexo"]
    )
    
    return BlockingPhaseContext(
        phase_name="fase_integracao_adapters",
        rules=rules,
        strong_match_score_threshold=0.6,
        candidate_limit=5,
        target_fields=target_config
    )


def test_pipeline_flow_from_es_search_to_spark_scoring(spark, es_client, integration_phase_context):
    
    # Adicionada a injeção do Search Executor na nova assinatura
    search_adapter = SparkESSearchAdapter(
        index_name=es_client["index"],
        es_config={"es_connection_url": es_client["url"]},
        search_executor=MultiSearchExecutor()
    )
    scoring_adapter = SparkScoringAdapter()

    df_source = spark.createDataFrame([
        Row(id_origem="A_REG", nome="Carlos Rocha", idade=45, cidade="Salvador")
    ])

    df_candidates = search_adapter.get_candidates(df_source, integration_phase_context)

    assert "source_record" in df_candidates.columns
    assert "candidate_record" in df_candidates.columns

    df_final_scored = scoring_adapter.calculate_score(df_candidates, integration_phase_context)
    results = df_final_scored.collect()

    assert len(results) == 1
    pair = results[0]

    assert pair.source_id_origem == "A_REG"
    assert pair.source_nome == "Carlos Rocha"
    assert pair.source_idade == 45
    assert pair.source_cidade == "Salvador"

    assert pair.candidate_id_nacional == "100"
    assert pair.candidate_nome_completo == "Carlos Rocha"
    assert pair.candidate_idade == "45"

    assert pair.match_score == 1.0