from abc import ABC, abstractmethod
from typing import Any


class DataPersistencePort(ABC):
    """Porta de persistência do resultado final da deduplicação."""

    @abstractmethod
    def save(self, df: Any) -> None:
        """Persiste o DataFrame deduplicado no destino configurado no adapter.

        Args:
            df: DataFrame com os registros originais enriquecidos com o cluster_id.
        """
        pass
