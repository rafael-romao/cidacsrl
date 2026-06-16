import pytest
from pyspark.sql import SparkSession, Row
from core.cidacsrl.infra.adapters.outbound.spark_data_transformation_adapter import SparkDataTransformationAdapter

@pytest.fixture(scope="module")
def spark():
    """Cria uma sessão local e isolada do Spark para testes de transformação."""
    session = SparkSession.builder \
        .appName("cidacsrl-test-data-transformation") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield session
    session.stop()

def test_add_phase_marker_appends_literal_column_correctly(spark):
    adapter = SparkDataTransformationAdapter()
    df_input = spark.createDataFrame([Row(source_id="1", candidate_id="100")])
    
    df_result = adapter.add_phase_marker(df_input, "fase_e2e_nome")
    row = df_result.collect()[0]
    
    assert "phase_match" in df_result.columns
    assert row["phase_match"] == "fase_e2e_nome"


def test_filter_matches_by_threshold(spark):
    """Mantém a característica original do teste de threshold solicitado pelo usuário."""
    adapter = SparkDataTransformationAdapter()
    
    data = [
        ("1", 0.95),  # Deve ser mantido (>= 0.9)
        ("2", 0.90),  # Deve ser mantido (>= 0.9)
        ("3", 0.85),  # Deve ser removido (< 0.9)
        ("4", None)   # Deve ser removido (Nulo)
    ]
    df_scored_pairs = spark.createDataFrame(data, ["source_id", "match_score"])
    
    df_filtered = adapter.filter_matches_by_threshold(dataset=df_scored_pairs, threshold=0.9)
    result_ids = [row["source_id"] for row in df_filtered.collect()]
    
    assert "1" in result_ids
    assert "2" in result_ids
    assert "3" not in result_ids
    assert "4" not in result_ids
    assert len(result_ids) == 2


def test_union_results_consolidates_multiple_dataframes_sequentially(spark):
    """Garante que a consolidação unifique diferentes fases em um RDD unificado."""
    adapter = SparkDataTransformationAdapter()
    
    # Cria três fatias de dados estruturalmente idênticas (saídas hipotéticas de fases)
    df_phase_1 = spark.createDataFrame([Row(source_id="1", match_score=0.99)])
    df_phase_2 = spark.createDataFrame([Row(source_id="2", match_score=0.95)])
    df_phase_3 = spark.createDataFrame([Row(source_id="3", match_score=0.91)])
    
    # Consolida as saídas através do adapter
    df_unified = adapter.union_results([df_phase_1, df_phase_2, df_phase_3])
    
    result_ids = [row["source_id"] for row in df_unified.collect()]
    
    # Valida que todos os registros foram empilhados com sucesso
    assert "1" in result_ids
    assert "2" in result_ids
    assert "3" in result_ids
    assert df_unified.count() == 3


def test_union_results_raises_value_error_when_list_is_empty(spark):
    """Garante comportamento defensivo caso o pipeline forneça uma lista vazia."""
    adapter = SparkDataTransformationAdapter()
    
    with pytest.raises(ValueError) as exc_info:
        adapter.union_results([])
        
    assert "Nenhum DataFrame válido" in str(exc_info.value)