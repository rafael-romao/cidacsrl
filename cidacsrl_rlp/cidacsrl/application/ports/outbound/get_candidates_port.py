from abc import ABC, abstractmethod
from typing import Any


class GetCandidatesPort(ABC):
    @abstractmethod
    def get_candidates(self, data: Any, blocking_config: Any) -> Any:
        pass