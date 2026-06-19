from abc import ABC, abstractmethod


class IndexingTelemetryPort(ABC):

    @abstractmethod
    def log_indexing_start(self, source_table: str, index_name: str, column_count: int) -> None:
        pass

    @abstractmethod
    def log_index_ensured(self, source_table: str, index_name: str, duration: float) -> None:
        pass

    @abstractmethod
    def log_indexing_completion(self, source_table: str, index_name: str, total_duration: float) -> None:
        pass
