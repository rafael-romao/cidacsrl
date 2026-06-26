from abc import ABC, abstractmethod
from typing import Dict, List


class SearchExecutor(ABC):
    """Contrato para execução de queries no Elasticsearch (single ou msearch)."""

    @abstractmethod
    def execute(self, es_client, index: str, queries: List[Dict]) -> List[Dict]:
        """Executa uma lista de queries e retorna os resultados brutos do ES.

        Args:
            es_client: Cliente Elasticsearch autenticado.
            index: Nome do índice a consultar.
            queries: Lista de corpos de query no formato Query DSL.

        Returns:
            Lista de respostas ES, uma por query executada.
        """
        pass