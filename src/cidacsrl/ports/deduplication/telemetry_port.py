from abc import ABC, abstractmethod


class DeduplicationTelemetryPort(ABC):
    """Contrato de telemetria para o pipeline de deduplicação."""

    @abstractmethod
    def log_deduplication_start(self, id_source: str, id_target: str, output_col: str) -> None:
        """Registra o início do processo de deduplicação.

        Args:
            id_source: Nome da coluna de ID de origem nos pares.
            id_target: Nome da coluna de ID de destino nos pares.
            output_col: Nome da coluna de saída para o cluster_id.
        """
        pass

    @abstractmethod
    def log_pairs_loaded(self, duration: float) -> None:
        """Registra o carregamento dos pares linkados.

        Args:
            duration: Tempo em segundos para carregar os pares.
        """
        pass

    @abstractmethod
    def log_clusters_found(self, duration: float) -> None:
        """Registra a conclusão do algoritmo de componentes conectados.

        Args:
            duration: Tempo em segundos para encontrar os clusters.
        """
        pass

    @abstractmethod
    def log_deduplication_completion(self, total_duration: float) -> None:
        """Registra a conclusão do pipeline de deduplicação.

        Args:
            total_duration: Tempo total em segundos do pipeline completo.
        """
        pass
