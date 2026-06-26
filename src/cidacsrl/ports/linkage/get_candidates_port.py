from abc import ABC, abstractmethod
from typing import Any

from cidacsrl.domain.linkage.linkage_specification import BlockingPhaseContext


class GetCandidatesPort(ABC):
    """Contrato para busca de registros candidatos ao pareamento via Elasticsearch."""

    @abstractmethod
    def get_candidates(self, data: Any, phase_context: BlockingPhaseContext) -> Any:
        """Executa as queries de blocagem e retorna pares (fonte, candidato).

        Args:
            data: DataFrame com os registros de origem a consultar.
            phase_context: Contexto da fase com regras de comparação e filtros.

        Returns:
            DataFrame com os pares (registro fonte, candidato ES) encontrados.
        """
        pass