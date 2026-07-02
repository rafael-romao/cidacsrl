from abc import ABC, abstractmethod


class TelemetryPort(ABC):
    """Contrato de telemetria para o pipeline de record linkage."""

    @abstractmethod
    def log_job_start(self, job_id: str, project_name: str, total_units: int) -> None:
        """Registra o início de um job de linkage.

        Args:
            job_id: Identificador único do job.
            project_name: Nome do projeto de linkage.
            total_units: Número total de work units a processar.
        """
        pass

    @abstractmethod
    def log_work_unit_start(self, job_id: str, unit_id: str, pending_count: int) -> None:
        """Registra o início do processamento de uma work unit.

        Args:
            job_id: Identificador único do job.
            unit_id: Identificador da work unit.
            pending_count: Número de work units ainda pendentes após esta.
        """
        pass

    @abstractmethod
    def log_phase_skipped(self, job_id: str, unit_id: str, phase_index: int, phase_name: str) -> None:
        """Registra que uma fase foi pulada por estar desabilitada.

        Args:
            job_id: Identificador único do job.
            unit_id: Identificador da work unit.
            phase_index: Índice numérico da fase (1-based).
            phase_name: Nome da fase pulada.
        """
        pass

    @abstractmethod
    def log_phase_exhausted(self, job_id: str, unit_id: str, phase_index: int, phase_name: str) -> None:
        """Registra encerramento antecipado da work unit por ausência de registros restantes.

        Args:
            job_id: Identificador único do job.
            unit_id: Identificador da work unit.
            phase_index: Índice numérico da fase (1-based).
            phase_name: Nome da fase onde o esgotamento foi detectado.
        """
        pass

    @abstractmethod
    def log_phase_telemetry(
        self,
        job_id: str,
        unit_id: str,
        phase_index: int,
        phase_name: str,
        records_in: int,
        candidates_found: int,
        records_out: int,
        duration: float,
        search_duration: float,
        persist_duration: float,
    ) -> None:
        """Registra as métricas de execução de uma fase de blocagem.

        Args:
            job_id: Identificador único do job.
            unit_id: Identificador da work unit.
            phase_index: Índice numérico da fase (1-based).
            phase_name: Nome da fase.
            records_in: Registros de origem no início da fase.
            candidates_found: Candidatos retornados pelo Elasticsearch.
            records_out: Pares com score acima do threshold persistidos.
            duration: Tempo total da fase em segundos.
            search_duration: Tempo da sub-fase de busca ES em segundos.
            persist_duration: Tempo da sub-fase de persistência em segundos.
        """
        pass

    @abstractmethod
    def log_work_unit_completion(
        self, job_id: str, unit_id: str, total_links: int, remaining: int, duration: float
    ) -> None:
        """Registra a conclusão bem-sucedida de uma work unit.

        Args:
            job_id: Identificador único do job.
            unit_id: Identificador da work unit.
            total_links: Total de pares linkados na work unit.
            remaining: Registros de origem sem match após todas as fases.
            duration: Tempo total da work unit em segundos.
        """
        pass

    @abstractmethod
    def log_work_unit_failure(
        self, job_id: str, unit_id: str, error_message: str, duration: float
    ) -> None:
        """Registra a falha de uma work unit.

        Args:
            job_id: Identificador único do job.
            unit_id: Identificador da work unit.
            error_message: Descrição do erro ocorrido.
            duration: Tempo até a falha em segundos.
        """
        pass

    @abstractmethod
    def log_job_completion(self, job_id: str, total_units: int, duration: float) -> None:
        """Registra a conclusão do job de linkage.

        Args:
            job_id: Identificador único do job.
            total_units: Total de work units processadas.
            duration: Tempo total do job em segundos.
        """
        pass
