import pytest
from testcontainers.elasticsearch import ElasticSearchContainer
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, Row
from pyspark import SparkContext

from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_search_adapter import SparkESSearchAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_scoring_adapter import SparkScoringAdapter
from cidacsrl_rlp.cidacsrl.domain.models.matching_rules import ComparisonRule
from cidacsrl_rlp.cidacsrl.domain.models.linkage_specification import BlockingPhaseContext, BlockingPhaseTargetFields

pytestmark = pytest.mark.integration


# ==========================================
# FIXTURES (Infraestrutura Combinada)
# ==========================================

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
    
    # Cadastramos candidatos com estruturas reais que batem com as regras de domínio
    docs = [
        {"id_nacional": "100", "nome_completo": "Carlos Rocha", "idade": 45, "sexo": "M"},
        {"id_nacional": "200", "nome_completo": "Carla Rocha", "idade": 42, "sexo": "F"}
    ]
    
    for doc in docs:
        client.index(index=index_name, id=doc["id_nacional"], document=doc)
        
    client.indices.refresh(index=index_name)
    return {"url": es_container, "index": index_name}

@pytest.fixture(scope="module")
def spark_session_local():
    """Garante uma sessão Spark limpa e destrói o contexto Java agressivamente no final."""
    if SparkContext._active_spark_context is not None:
        SparkContext._active_spark_context.stop()

    session = SparkSession.builder \
        .appName("pytest-pyspark-testing-linkage") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
        
    yield session
    session.stop()
    if SparkContext._active_spark_context is not None:
        SparkContext._active_spark_context.stop()


@pytest.fixture
def integration_phase_context():
    """Regras estritas que cruzam a busca do ES com a pontuação do Scoring."""
    rules = [
        ComparisonRule(source_column="nome", target_column="nome_completo", similarity="exact", weight=2.0),
        ComparisonRule(source_column="idade", target_column="idade", similarity="exact", weight=1.0)
    ]
    
    target_config = BlockingPhaseTargetFields(
        comparison_fields=["nome_completo", "idade"],
        required_fields=["id_nacional"],
        extra_fields=["sexo"]
    )
    
    return BlockingPhaseContext(
        phase_name="fase_integracao_adapters",
        rules=rules,
        strong_match_score_threshold=0.6,  # Permitirá match parcial se necessário
        candidate_limit=5,
        target_fields=target_config
    )


# ==========================================
# TESTE DE INTEGRAÇÃO DE FLUXO
# ==========================================

from pyspark.sql import SparkSession

def test_pipeline_flow_from_es_search_to_spark_scoring(spark_session_local, es_client, integration_phase_context):
    
    spark = spark_session_local
    
    search_adapter = SparkESSearchAdapter(
        index_name=es_client["index"],
        es_config={"es_connection_url": es_client["url"]}
    )
    scoring_adapter = SparkScoringAdapter()

    try:
        df_source = spark.createDataFrame([
            Row(id_origem="A_REG", nome="Carlos Rocha", idade=45, cidade="Salvador")
        ])

        df_candidates = search_adapter.get_candidates(df_source, integration_phase_context)

        assert "source_record" in df_candidates.columns
        assert "candidate_record" in df_candidates.columns

        df_final_scored = scoring_adapter.calculate_score(df_candidates, integration_phase_context)
        results = df_final_scored.collect()

        # ==========================================
        # VALIDAÇÕES DO SUCESSO DA INTEGRAÇÃO
        # ==========================================
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
        assert pair.sim_nome == 1.0
        assert pair.sim_idade == 1.0
    finally:
        spark.stop()
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()