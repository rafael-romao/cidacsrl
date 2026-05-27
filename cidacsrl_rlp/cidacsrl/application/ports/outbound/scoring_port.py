from abc import ABC, abstractmethod
from typing import Any

from cidacsrl_rlp.cidacsrl.domain.models.workflow import BlockingPhaseContext

class ScoringPort(ABC):
    @abstractmethod
    def calculate_score(self, df_candidates: Any, phase_context: BlockingPhaseContext) -> Any:
        pass