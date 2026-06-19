from abc import ABC, abstractmethod
from typing import Any, List


class DataTransformationPort(ABC):

    @abstractmethod
    def add_phase_marker(self, df: Any, phase_name: str) -> Any:
        pass

    @abstractmethod
    def filter_matches_by_threshold(self, dataset: Any, threshold: float) -> Any:
        pass

    @abstractmethod
    def union_results(self, phase_outputs: List[Any]) -> Any:
        pass