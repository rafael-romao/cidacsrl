from abc import ABC, abstractmethod
from typing import Any, Optional


class DataPersistencePort(ABC):

    @abstractmethod
    def save_phase_output(
        self,
        df: Any,
        project_name: str,
        phase_name: str,
        partition_column: Optional[str] = None,
    ) -> int:
        pass
