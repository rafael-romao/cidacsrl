from abc import ABC, abstractmethod
from typing import Any

from cidacsrl.domain.linkage.linkage_specification import BlockingPhaseContext


class ScoringPort(ABC):
    """Contrato para cálculo de scores de similaridade entre pares de registros."""

    @abstractmethod
    def calculate_score(
        self,
        df_candidates: Any,
        phase_context: BlockingPhaseContext,
        debug: bool = False,
    ) -> Any:
        """Calcula o score composto de similaridade para cada par candidato.

        Args:
            df_candidates: DataFrame com pares (fonte, candidato) a pontuar.
            phase_context: Contexto com regras de comparação e pesos.
            debug: Se True, inclui colunas auxiliares de similaridade por regra. Defaults to False.

        Returns:
            DataFrame com os pares enriquecidos com score e similaridades.
        """
        pass