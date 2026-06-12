from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class DataTransformationPort(ABC):
    
    @abstractmethod
    def exclude_records(self, primary_dataset: DataFrame, records_to_exclude: DataFrame, join_key: str) -> DataFrame:
        pass

    @abstractmethod
    def filter_matches_by_threshold(self, dataset: DataFrame, threshold: float) -> DataFrame:
        pass