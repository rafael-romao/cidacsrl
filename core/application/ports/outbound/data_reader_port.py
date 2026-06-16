from abc import ABC, abstractmethod
from typing import Any

class DataReaderPort(ABC):
    @abstractmethod
    def read_data(self) -> Any:
        pass