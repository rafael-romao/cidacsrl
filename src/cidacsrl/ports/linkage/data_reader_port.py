from abc import ABC, abstractmethod
from typing import Any


class DataReaderPort(ABC):
    """Contrato para leitura de dados de entrada do pipeline de linkage."""

    @abstractmethod
    def read_data(self) -> Any:
        """Carrega e retorna os dados de entrada como DataFrame.

        Returns:
            DataFrame com os dados de entrada.
        """
        pass