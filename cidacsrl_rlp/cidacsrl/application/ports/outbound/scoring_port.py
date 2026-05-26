from abc import ABC, abstractmethod
from typing import Any

class ScoringPort(ABC):
    @abstractmethod
    def calculate_score(self, df_candidates: Any, phase: Any) -> Any:
        pass