from abc import ABC, abstractmethod
from typing import Any
from core.application.domain.models.indexing_specification import DatasetIndexingSpecification

class DataIndexingPort(ABC):
    @abstractmethod
    def ensure_index_with_mapping(self, spec: DatasetIndexingSpecification) -> None:
        pass

    @abstractmethod
    def index_dataframe(self, df: Any, spec: DatasetIndexingSpecification) -> None:
        pass