from abc import ABC, abstractmethod
from typing import Any, Dict, List, Set


class DataIngestionPort(ABC):
    """
    Porta de saída responsável pelo carregamento e inspeção
    física de volumes de dados brutos no storage do Spark.
    """

    @abstractmethod
    def check_health(self, source_table: str) -> List[str]:
        pass

    @abstractmethod
    def validate_source_schema(self, table_name: str, required_columns: Set[str]) -> None:
        pass

    @abstractmethod
    def discover_partitions(self, table_name: str, partition_column: str) -> List[str]:
        pass

    @abstractmethod
    def read_all(self, table_name: str) -> Any:
        pass

    @abstractmethod
    def read_slice(self, table_name: str, filters: Dict[str, Any]) -> Any:
        pass