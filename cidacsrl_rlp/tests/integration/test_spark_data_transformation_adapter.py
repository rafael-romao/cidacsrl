from pyspark.sql import SparkSession
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_transformation_adapter import SparkDataTransformationAdapter

def test_exclude_records():
    spark = SparkSession.builder \
        .appName("pytest-pyspark-testing") \
        .master("local[2]") \
        .getOrCreate()
    try:
        adapter = SparkDataTransformationAdapter()
        df_primary = spark.createDataFrame([("1", "A"), ("2", "B"), ("3", "C")], ["id", "val"])
        df_exclude = spark.createDataFrame([("2",)], ["id"])
        df_result = adapter.exclude_records(df_primary, df_exclude, "id")
        result_ids = [row["id"] for row in df_result.collect()]
        assert "2" not in result_ids
        assert len(result_ids) == 2
    finally:
        spark.stop()