from abc import ABC, abstractmethod
from typing import Any

from cidacsrl.domain.linkage.linkage_specification import BlockingPhaseContext


class ScoringPort(ABC):
    @abstractmethod
    def calculate_score(
        self,
        df_candidates: Any,
        phase_context: BlockingPhaseContext,
        debug: bool = False,
    ) -> Any:
        pass