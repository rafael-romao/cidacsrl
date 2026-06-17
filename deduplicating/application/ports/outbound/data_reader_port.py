from abc import ABC, abstractmethod
from typing import Any


class DataReaderPort(ABC):
    """Porta de leitura do arquivo de pares linkados."""

    @abstractmethod
    def read_linked_pairs(self) -> Any:
        """Carrega o dataset de pares linkados e retorna um DataFrame Spark."""
        pass
