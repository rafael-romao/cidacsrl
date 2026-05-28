from abc import ABC, abstractmethod
from typing import Any

class DataTransformationPort(ABC):
    
    @abstractmethod
    def exclude_records(self, primary_dataset: Any, records_to_exclude: Any, join_key: str) -> Any:
        pass