import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from core.cidacsrl.infra.adapters.outbound.spark_scoring_adapter import SparkScoringAdapter
from core.cidacsrl.domain.models.matching_rules import ComparisonRule
from core.cidacsrl.domain.models.linkage_specification import BlockingPhaseContext, BlockingPhaseTargetFields


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("cidacsrl-test-scoring")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark


@pytest.fixture
def adapter():
    return SparkScoringAdapter()


@pytest.fixture
def sample_rules():
    return [
        ComparisonRule(source_column="nome", target_column="nome_completo", similarity="exact", weight=2.0, es_clause_type="must", penalty=0.0),
        ComparisonRule(source_column="idade", target_column="idade", similarity="exact", weight=1.0, es_clause_type="must", penalty=0.0)
    ]


@pytest.fixture
def real_phase_context(sample_rules):
    target_config = BlockingPhaseTargetFields(
        comparison_fields=["nome_completo", "idade"],
        extra_fields=["sexo"]
    )
    return BlockingPhaseContext(
        phase_name="fase_teste_scoring",
        rules=sample_rules,
        strong_match_score_threshold=0.5,
        target_fields=target_config,
    )


@pytest.fixture
def candidates_df(spark):
    schema = StructType([
        StructField("source_record", StructType([
            StructField("nome", StringType()),
            StructField("idade", IntegerType()),
            StructField("municipio_codigo", StringType())
        ])),
        StructField("candidate_record", StructType([
            StructField("nome_completo", StringType()),
            StructField("idade", IntegerType()),
            StructField("sexo", StringType()),
            StructField("_score", FloatType())
        ])),
        StructField("phase_match", StringType())
    ])
    
    data = [
        Row(
            source_record=Row(nome="João", idade=30, municipio_codigo="2927408"),
            candidate_record=Row(nome_completo="João", idade=30, sexo="M", _score=15.5),
            phase_match="fase_teste_scoring"
        ),
        Row(
            source_record=Row(nome="Maria", idade=25, municipio_codigo="2927408"),
            candidate_record=Row(nome_completo="Mariana", idade=25, sexo="F", _score=8.2),
            phase_match="fase_teste_scoring"
        )
    ]
    return spark.createDataFrame(data, schema)


def test_build_score_schema(adapter, real_phase_context):
    schema = adapter._build_score_schema(real_phase_context)
    field_names = [f.name for f in schema.fields]
    
    assert "match_score" in field_names
    assert "sim_nome" in field_names
    assert "sim_idade" in field_names
    
    for field in schema.fields:
        assert isinstance(field.dataType, FloatType)


def test_calculate_score_distributed_udf_execution(adapter, real_phase_context, candidates_df):
    scored_df = adapter.calculate_score(candidates_df, real_phase_context)
    results = scored_df.collect()
    
    assert len(results) == 1
    row = results[0]
    
    assert row.match_score == 1.0
    assert row.sim_nome == 1.0
    assert row.sim_idade == 1.0


def test_calculate_score_dynamic_source_prefix_from_schema(adapter, real_phase_context, candidates_df):
    scored_df = adapter.calculate_score(candidates_df, real_phase_context)
    colunas = scored_df.columns
    
    assert "source_nome" in colunas
    assert "source_idade" in colunas
    assert "source_municipio_codigo" in colunas
    
    row = scored_df.first()
    assert row.source_municipio_codigo == "2927408"


def test_calculate_score_domain_driven_candidate_prefix(adapter, real_phase_context, candidates_df):
    scored_df = adapter.calculate_score(candidates_df, real_phase_context)
    colunas = scored_df.columns
    
    assert "candidate_nome_completo" in colunas
    assert "candidate_idade" in colunas
    assert "candidate_sexo" in colunas
    
    row = scored_df.first()
    assert row.candidate_sexo == "M"


def test_calculate_score_with_empty_dataframe(adapter, real_phase_context, candidates_df):
    empty_df = candidates_df.filter("1 = 0")
    scored_df = adapter.calculate_score(empty_df, real_phase_context)
    
    assert len(scored_df.collect()) == 0
    assert "match_score" in scored_df.columns


def test_calculate_score_with_debug_column(adapter, real_phase_context, candidates_df):
    scored_df = adapter.calculate_score(candidates_df, real_phase_context, debug=True)

    assert "score_debug_json" in scored_df.columns
    row = scored_df.first()
    assert row.score_debug_json is not None
    assert '"rules"' in row.score_debug_json