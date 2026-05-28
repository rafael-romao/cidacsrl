from abc import ABC, abstractmethod
from typing import Any, Optional

class DataPersistencePort(ABC):

    @abstractmethod
    def write_data(self, data: Any, path: str, data_format: str, partition_cols: Optional[list[str]] = None) -> None:
        pass