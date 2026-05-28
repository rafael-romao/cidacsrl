from abc import ABC, abstractmethod
from typing import Any, Optional

class DataIngestionPort(ABC):
    
    @abstractmethod
    def read_data(self, path: str, data_format: str, partition_col: Optional[str] = None, partition_val: Optional[Any] = None) -> Any:
        pass

    @abstractmethod
    def read_target_data(self, path: str, data_format: str, **kwargs) -> Any:        
        pass

    @abstractmethod
    def get_partitioned_sample(self, path: str, sample_size: int) -> Any:        
        pass

    @abstractmethod
    def read_specific_partition(self, path: str, partition_column: str, partition_value: Any) -> Any:
        pass