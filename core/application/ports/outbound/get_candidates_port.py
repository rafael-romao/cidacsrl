from abc import ABC, abstractmethod
from typing import Any

from core.application.domain.models.linkage_specification import BlockingPhaseContext


class GetCandidatesPort(ABC):
    @abstractmethod
    def get_candidates(self, data: Any, phase_context: BlockingPhaseContext) -> Any:
        pass