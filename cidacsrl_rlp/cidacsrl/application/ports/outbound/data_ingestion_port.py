from abc import ABC, abstractmethod
from typing import Any, Optional

class DataIngestionPort(ABC):
    @abstractmethod
    def read_source_data(self, table_name: str, **kwargs) -> Any:
        pass

    @abstractmethod
    def read_target_data(self, index_name: str, **kwargs) -> Any:
        pass

    @abstractmethod
    def read_specific_partition(self, table_name: str, partition_expr: str, **kwargs) -> Any:
        pass

    @abstractmethod
    def get_partitioned_sample(self, table_name: str, fraction: float, seed: Optional[int] = None, **kwargs) -> Any:
        pass