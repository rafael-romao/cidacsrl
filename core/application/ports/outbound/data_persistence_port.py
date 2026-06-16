from abc import ABC, abstractmethod
from typing import Any

class DataPersistencePort(ABC):

    @abstractmethod
    def save_phase_output(
        self, 
        df: Any, 
        project_name: str, 
        job_id: str, 
        unit_id: str, 
        phase_name: str
    ) -> int:
        pass