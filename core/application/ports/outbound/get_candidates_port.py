from abc import ABC, abstractmethod
from typing import Any

from core.domain.models.linkage_specification import BlockingPhaseContext


class GetCandidatesPort(ABC):
    @abstractmethod
    def get_candidates(self, data: Any, phase_context: BlockingPhaseContext) -> Any:
        pass