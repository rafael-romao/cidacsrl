import pytest
from pathlib import Path
from pyspark.sql import SparkSession

from core.infra.adapters.outbound.spark_data_ingestion_adapter import SparkDataIngestionAdapter
from cidacsrl.config.models.storage_config import SourceStorageConfig

@pytest.fixture(scope="module")
def local_spark():
    """Cria uma sessão local e isolada do Spark para este módulo de integração."""
    spark = SparkSession.builder \
        .appName("CIDACS-RL Integration Testing Ingestion") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_dataframe(local_spark):
    data = [
        {"codigo_internacao": "I01", "nome_completo": "Maria", "uf_internacao": "BA"},
        {"codigo_internacao": "I02", "nome_completo": "João", "uf_internacao": "SP"},
        {"codigo_internacao": "I03", "nome_completo": "Ana", "uf_internacao": "BA"},
    ]
    return local_spark.createDataFrame(data)

@pytest.fixture
def simulated_storage_config(tmp_path):
    source_dir = tmp_path / "source_data"
    source_dir.mkdir()
    
    return {
        "config": SourceStorageConfig(
            source_path=str(source_dir),
            source_format="parquet"
        ),
        "source_dir": source_dir
    }

def test_discover_partitions_returns_sorted_distinct_values_from_parquet(
    local_spark, 
    sample_dataframe, 
    simulated_storage_config
):
    table_name = "internacao_discovery_test"
    physical_table_path = simulated_storage_config["source_dir"] / table_name
    sample_dataframe.write.format("parquet").save(str(physical_table_path))
    
    # Nova assinatura limpa (apenas infraestrutura)
    adapter = SparkDataIngestionAdapter(
        spark_session=local_spark,
        storage_config=simulated_storage_config["config"]
    )
    
    discovered_partitions = adapter.discover_partitions(table_name, "uf_internacao")
    assert discovered_partitions == ["BA", "SP"]

def test_read_slice_applies_logical_filters_correctly(
    local_spark, 
    sample_dataframe, 
    simulated_storage_config
):
    table_name = "internacao_slice_test"
    physical_table_path = simulated_storage_config["source_dir"] / table_name
    sample_dataframe.write.format("parquet").save(str(physical_table_path))
    
    adapter = SparkDataIngestionAdapter(
        spark_session=local_spark,
        storage_config=simulated_storage_config["config"]
    )
    
    ba_df = adapter.read_slice(table_name, {"uf_internacao": "BA"})
    ba_rows = ba_df.collect()
    assert len(ba_rows) == 2
    for row in ba_rows:
        assert row["uf_internacao"] == "BA"

def test_read_all_returns_entire_dataframe_without_filters(
    local_spark,
    sample_dataframe,
    simulated_storage_config
):
    table_name = "internacao_read_all_test"
    physical_table_path = simulated_storage_config["source_dir"] / table_name
    sample_dataframe.write.format("parquet").save(str(physical_table_path))

    adapter = SparkDataIngestionAdapter(
        spark_session=local_spark,
        storage_config=simulated_storage_config["config"]
    )

    full_df = adapter.read_all(table_name)
    assert full_df.count() == 3

def test_validate_source_schema_succeeds_when_all_required_columns_exist(
    local_spark,
    sample_dataframe,
    simulated_storage_config
):
    table_name = "internacao_schema_valid_test"
    physical_table_path = simulated_storage_config["source_dir"] / table_name
    sample_dataframe.write.format("parquet").save(str(physical_table_path))

    adapter = SparkDataIngestionAdapter(
        spark_session=local_spark,
        storage_config=simulated_storage_config["config"]
    )

    adapter.validate_source_schema(table_name, {"codigo_internacao", "nome_completo", "uf_internacao"})

def test_validate_source_schema_raises_when_column_is_missing(
    local_spark,
    sample_dataframe,
    simulated_storage_config
):
    table_name = "internacao_schema_missing_test"
    physical_table_path = simulated_storage_config["source_dir"] / table_name
    sample_dataframe.write.format("parquet").save(str(physical_table_path))

    adapter = SparkDataIngestionAdapter(
        spark_session=local_spark,
        storage_config=simulated_storage_config["config"]
    )

    with pytest.raises(ValueError, match="coluna_inexistente"):
        adapter.validate_source_schema(table_name, {"codigo_internacao", "coluna_inexistente"})