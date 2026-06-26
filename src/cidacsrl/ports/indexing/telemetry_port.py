from abc import ABC, abstractmethod


class IndexingTelemetryPort(ABC):
    """Contrato de telemetria para o pipeline de indexação."""

    @abstractmethod
    def log_indexing_start(self, source_table: str, index_name: str, column_count: int) -> None:
        """Registra o início do processo de indexação.

        Args:
            source_table: Nome da tabela de origem.
            index_name: Nome do índice Elasticsearch de destino.
            column_count: Número de colunas a serem indexadas.
        """
        pass

    @abstractmethod
    def log_index_ensured(self, source_table: str, index_name: str, duration: float) -> None:
        """Registra a criação ou verificação bem-sucedida do índice.

        Args:
            source_table: Nome da tabela de origem.
            index_name: Nome do índice criado ou verificado.
            duration: Tempo em segundos para garantir o índice.
        """
        pass

    @abstractmethod
    def log_indexing_completion(self, source_table: str, index_name: str, total_duration: float) -> None:
        """Registra a conclusão do pipeline de indexação.

        Args:
            source_table: Nome da tabela de origem.
            index_name: Nome do índice populado.
            total_duration: Tempo total em segundos do pipeline completo.
        """
        pass
