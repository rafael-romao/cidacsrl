import os
import pytest
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_persistence_adapter import SparkDataPersistenceAdapter
from cidacsrl_rlp.cidacsrl.infra.configs.models.storage_config import OutputStorageConfig

@pytest.fixture
def mock_output_config(tmp_path):
    return OutputStorageConfig(
        output_data_path=str(tmp_path / "output_data"),
        output_data_format="parquet"
    )

@pytest.fixture(scope="module")
def spark_session_local():
    """Garante uma sessão Spark limpa e destrói o contexto Java agressivamente no final."""
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    
    if SparkContext._active_spark_context is not None:
        SparkContext._active_spark_context.stop()

    spark = SparkSession.builder \
        .appName("pytest-pyspark-testing-data-persistente") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate() 
    
    yield spark
    spark.stop()
    if SparkContext._active_spark_context is not None:
        SparkContext._active_spark_context.stop()


def test_write_data_success(spark_session_local, mock_output_config):
    adapter = SparkDataPersistenceAdapter(spark_session=spark_session_local, config=mock_output_config)
    output_folder = "test_output"
    
    df = spark_session_local.createDataFrame([("A", 1)], ["id", "val"])
    adapter.write_data(df, output_folder)
    
    physical_path = os.path.join(mock_output_config.output_data_path, output_folder)
    assert os.path.exists(physical_path)
    
    saved_df = spark_session_local.read.parquet(physical_path)
    assert saved_df.count() == 1