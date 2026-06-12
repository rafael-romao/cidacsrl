import pytest
from pyspark.sql import SparkSession
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_transformation_adapter import SparkDataTransformationAdapter

@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder \
        .appName("cidacsrl-test-data-transformation") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield session

def test_exclude_records(spark):
    adapter = SparkDataTransformationAdapter()
    
    df_primary = spark.createDataFrame([("1", "A"), ("2", "B"), ("3", "C")], ["id", "val"])
    
    # ATENÇÃO: O adapter espera que a coluna no records_to_exclude tenha o prefixo 'source_'
    df_exclude = spark.createDataFrame([("2",)], ["source_id"])
    
    df_result = adapter.exclude_records(primary_dataset=df_primary, records_to_exclude=df_exclude, join_key="id")
    result_ids = [row["id"] for row in df_result.collect()]
    
    assert "2" not in result_ids
    assert len(result_ids) == 2


def test_filter_matches_by_threshold(spark):
    adapter = SparkDataTransformationAdapter()
    
    # Simula o DataFrame que sai da fase de scoring
    data = [
        ("1", 0.95),  # Deve ser mantido (>= 0.9)
        ("2", 0.90),  # Deve ser mantido (>= 0.9)
        ("3", 0.85),  # Deve ser removido (< 0.9)
        ("4", None)   # Deve ser removido (Nulo)
    ]
    df_scored_pairs = spark.createDataFrame(data, ["source_id", "match_score"])
    
    # Executa o filtro com threshold 0.9
    df_filtered = adapter.filter_matches_by_threshold(dataset=df_scored_pairs, threshold=0.9)
    result_ids = [row["source_id"] for row in df_filtered.collect()]
    
    # Verifica se apenas os matches fortes sobraram
    assert "1" in result_ids
    assert "2" in result_ids
    assert "3" not in result_ids
    assert "4" not in result_ids
    assert len(result_ids) == 2