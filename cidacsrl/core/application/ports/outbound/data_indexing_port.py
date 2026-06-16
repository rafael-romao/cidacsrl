from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from cidacsrl.cidacsrl.domain.models.indexing_specification import DatasetIndexingSpecification

class DataIndexingPort(ABC):
    @abstractmethod
    def ensure_index_with_mapping(self, spec: DatasetIndexingSpecification) -> None:
        pass

    @abstractmethod
    def index_dataframe(self, df: DataFrame, spec: DatasetIndexingSpecification) -> None:
        pass