from abc import ABC, abstractmethod
from typing import List
from pyspark.sql import DataFrame

class DataTransformationPort(ABC):

    @abstractmethod
    def add_phase_marker(self, df: DataFrame, phase_name: str) -> DataFrame:
        pass

    @abstractmethod
    def filter_matches_by_threshold(self, dataset: DataFrame, threshold: float) -> DataFrame:
        pass

    @abstractmethod
    def union_results(self, phase_outputs: List[DataFrame]) -> DataFrame:
        pass