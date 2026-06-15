from abc import ABC, abstractmethod
from typing import Any, List

class DataPersistencePort(ABC):

    @abstractmethod
    def save_linkage_output(self, phase_outputs: List[Any], unit_id: str) -> int:
        pass