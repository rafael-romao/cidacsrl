from pyspark.sql import SparkSession, DataFrame


class SparkIOAdapter:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_csv(self, path: str, **options) -> DataFrame:
        return self.spark.read.options(**options).csv(path)

    def write_csv(self, df: DataFrame, path: str, **options):
        df.write.options(**options).csv(path)
    
    def read_parquet(self, path: str, **options) -> DataFrame:
        return self.spark.read.options(**options).parquet(path)

    def write_parquet(self, df: DataFrame, path: str, **options):
        df.write.options(**options).parquet(path)