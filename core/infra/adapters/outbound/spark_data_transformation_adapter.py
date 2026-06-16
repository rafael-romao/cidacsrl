import functools
from typing import List
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from core.application.ports.outbound.data_transformation_port import DataTransformationPort


class SparkDataTransformationAdapter(DataTransformationPort):

    def add_phase_marker(self, df: DataFrame, phase_name: str) -> DataFrame:
        return df.withColumn("phase_match", F.lit(phase_name))

    
    def exclude_records(self, primary_dataset: DataFrame, records_to_exclude: DataFrame, join_key: str) -> DataFrame:
        join_condition = primary_dataset[join_key] == records_to_exclude[f"source_{join_key}"]
        return primary_dataset.join(records_to_exclude, on=join_condition, how="left_anti")
    
    
    def filter_matches_by_threshold(self, dataset: DataFrame, threshold: float) -> DataFrame:
        """Aplica o filtro de corte com tolerância a valores nulos de pontuação."""
        return dataset.filter(
            (F.col("match_score").isNotNull()) & 
            (F.col("match_score") >= F.lit(threshold))
        )

    def union_results(self, phase_outputs: List[DataFrame]) -> DataFrame:
        valid_dfs = [df for df in phase_outputs if df is not None]
        if not valid_dfs:
            raise ValueError("Nenhum DataFrame válido foi fornecido para consolidação de resultados.")
        return functools.reduce(lambda df1, df2: df1.unionAll(df2), valid_dfs)