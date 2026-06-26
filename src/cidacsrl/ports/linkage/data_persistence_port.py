from abc import ABC, abstractmethod
from typing import Any, Optional


class DataPersistencePort(ABC):
    """Contrato para persistência dos pares gerados em cada fase de blocagem."""

    @abstractmethod
    def save_phase_output(
        self,
        df: Any,
        project_name: str,
        phase_name: str,
        partition_column: Optional[str] = None,
    ) -> int:
        """Persiste os pares de uma fase e retorna o número de registros salvos.

        Args:
            df: DataFrame com os pares de registros pareados na fase.
            project_name: Nome do projeto de linkage (usado no caminho de saída).
            phase_name: Nome da fase de blocagem (usado no caminho de saída).
            partition_column: Coluna para particionamento dinâmico do output. Defaults to None.

        Returns:
            Número de registros persistidos.
        """
        pass
