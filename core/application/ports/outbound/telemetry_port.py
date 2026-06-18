from abc import ABC, abstractmethod


class TelemetryPort(ABC):

    @abstractmethod
    def log_job_start(self, job_id: str, project_name: str, total_units: int) -> None:
        pass

    @abstractmethod
    def log_work_unit_start(self, job_id: str, unit_id: str, pending_count: int) -> None:
        pass

    @abstractmethod
    def log_phase_telemetry(
        self,
        job_id: str,
        unit_id: str,
        phase_index: int,
        phase_name: str,
        records_in: int,
        records_out: int,
        duration: float,
    ) -> None:
        pass

    @abstractmethod
    def log_work_unit_completion(
        self, job_id: str, unit_id: str, total_links: int, remaining: int, duration: float
    ) -> None:
        pass

    @abstractmethod
    def log_job_completion(self, job_id: str, total_units: int, duration: float) -> None:
        pass
