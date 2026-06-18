from abc import ABC, abstractmethod


class DeduplicationTelemetryPort(ABC):

    @abstractmethod
    def log_deduplication_start(self, id_source: str, id_target: str, output_col: str) -> None:
        pass

    @abstractmethod
    def log_pairs_loaded(self, duration: float) -> None:
        pass

    @abstractmethod
    def log_clusters_found(self, duration: float) -> None:
        pass

    @abstractmethod
    def log_deduplication_completion(self, total_duration: float) -> None:
        pass
