import os
import pytest
from pyspark.sql import SparkSession
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_ingestion_adapter import SparkDataIngestionAdapter
from cidacsrl_rlp.cidacsrl.infra.configs.models.storage_config import SourceStorageConfig

@pytest.fixture
def mock_source_config(tmp_path):
    return SourceStorageConfig(
        source_data_path=str(tmp_path / "source_data"),
        source_data_format="parquet"
    )


def make_ingestion_adapter(spark, mock_source_config):
    return SparkDataIngestionAdapter(spark, mock_source_config)

def test_check_health_success(mock_source_config):
    spark = SparkSession.builder \
        .appName("pytest-pyspark-testing") \
        .master("local[2]") \
        .getOrCreate()
    try:
        ingestion_adapter = make_ingestion_adapter(spark, mock_source_config)
        source_table = "valid_input"
        input_path = os.path.join(mock_source_config.source_data_path, source_table)
        dummy_df = spark.createDataFrame([("test",)], ["col"])
        dummy_df.write.mode("overwrite").format("parquet").save(input_path)

        errors = ingestion_adapter.check_health(source_table=source_table)
        assert len(errors) == 0
    finally:
        spark.stop()

def test_check_health_input_missing_returns_error(mock_source_config):
    spark = SparkSession.builder \
        .appName("pytest-pyspark-testing") \
        .master("local[2]") \
        .getOrCreate()
    try:
        ingestion_adapter = make_ingestion_adapter(spark, mock_source_config)
        errors = ingestion_adapter.check_health(source_table="tabela_inexistente")
        assert len(errors) == 1
        assert "Erro ao acessar dados de origem" in errors[0]
    finally:
        spark.stop()

def test_read_source_data(mock_source_config):
    spark = SparkSession.builder \
        .appName("pytest-pyspark-testing") \
        .master("local[2]") \
        .getOrCreate()
    try:
        ingestion_adapter = make_ingestion_adapter(spark, mock_source_config)
        table_name = "test_table"
        physical_path = os.path.join(mock_source_config.source_data_path, table_name)
        spark.createDataFrame([("A", 1)], ["id", "val"]).write.parquet(physical_path)
        df = ingestion_adapter.read_source_data(table_name)
        assert df.count() == 1
        assert "val" in df.columns
    finally:
        spark.stop()