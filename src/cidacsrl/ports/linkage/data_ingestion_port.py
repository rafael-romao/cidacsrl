from abc import ABC, abstractmethod
from typing import Any, Dict, List, Set


class DataIngestionPort(ABC):
    """
    Porta de saída responsável pelo carregamento e inspeção
    física de volumes de dados brutos no storage do Spark.
    """

    @abstractmethod
    def check_health(self, source_table: str) -> List[str]:
        """Verifica se o caminho da tabela de origem é acessível.

        Args:
            source_table: Nome da tabela de origem.

        Returns:
            Lista de erros encontrados; vazia se saudável.
        """
        pass

    @abstractmethod
    def validate_source_schema(self, table_name: str, required_columns: Set[str]) -> None:
        """Valida que todas as colunas requeridas existem na tabela.

        Args:
            table_name: Nome da tabela a validar.
            required_columns: Conjunto de colunas que devem estar presentes.

        Raises:
            ValueError: Se alguma coluna requerida estiver ausente.
        """
        pass

    @abstractmethod
    def discover_partitions(self, table_name: str, partition_column: str) -> List[str]:
        """Escaneia a coluna de partição e retorna os valores distintos disponíveis.

        Args:
            table_name: Nome da tabela a escanear.
            partition_column: Nome da coluna de partição.

        Returns:
            Lista de valores de partição encontrados.
        """
        pass

    @abstractmethod
    def read_all(self, table_name: str) -> Any:
        """Lê a tabela de origem completa como DataFrame.

        Args:
            table_name: Nome da tabela a ler.

        Returns:
            DataFrame com todos os registros da tabela.
        """
        pass

    @abstractmethod
    def read_slice(self, table_name: str, filters: Dict[str, Any]) -> Any:
        """Lê um subconjunto filtrado da tabela de origem.

        Args:
            table_name: Nome da tabela a ler.
            filters: Dicionário de coluna → valor para filtrar a leitura.

        Returns:
            DataFrame com os registros que satisfazem os filtros.
        """
        pass