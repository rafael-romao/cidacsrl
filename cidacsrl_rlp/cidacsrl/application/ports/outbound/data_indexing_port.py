from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from cidacsrl_rlp.cidacsrl.domain.models.indexing_specification import DatasetIndexingSpecification

class DataIndexingPort(ABC):
    @abstractmethod
    def ensure_index_with_mapping(self, index_name: str, spec: DatasetIndexingSpecification) -> None:
        pass

    @abstractmethod
    def index_dataframe(self, df: DataFrame, index_name: str, id_field: str) -> None:
        pass