from abc import ABC, abstractmethod
from typing import Any

from cidacsrl.domain.indexing.indexing_specification import (
    DatasetIndexingSpecification,
)


class DataIndexingPort(ABC):
    """Contrato para criação de índices e ingestão de dados no Elasticsearch."""

    @abstractmethod
    def ensure_index_with_mapping(self, spec: DatasetIndexingSpecification) -> None:
        """Cria o índice com o mapeamento definido, se ainda não existir.

        Args:
            spec: Especificação completa do índice (nome, settings, colunas).
        """
        pass

    @abstractmethod
    def index_dataframe(self, df: Any, spec: DatasetIndexingSpecification) -> None:
        """Ingere um DataFrame no índice Elasticsearch.

        Args:
            df: DataFrame com os dados a indexar.
            spec: Especificação do índice de destino.
        """
        pass