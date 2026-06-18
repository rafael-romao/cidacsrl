from abc import ABC, abstractmethod
from typing import List
from core.domain.models.tracking.work_unit import WorkUnitExecutionRecord, WorkUnitStatus

class ExecutionTrackingPort(ABC):
    
    @abstractmethod
    def initialize_job_state(self, job_id: str, work_units: List[WorkUnitExecutionRecord]) -> None:
        pass

    @abstractmethod
    def get_pending_work_units(self, job_id: str) -> List[WorkUnitExecutionRecord]:
        pass

    @abstractmethod
    def get_all_work_units(self, job_id: str) -> List[WorkUnitExecutionRecord]:
        """Retorna todos os registros de execução de um job."""
        pass

    @abstractmethod
    def update_work_unit_status(
        self, 
        job_id: str, 
        unit_id: str, 
        status: WorkUnitStatus, 
        records_processed: int = 0,
        error_message: str = None
    ) -> None:        
        pass

    @abstractmethod
    def log_work_unit_start(self, job_id: str, unit_id: str, pending_count: int) -> None:
        """Imprime o cabeçalho estruturado de início de um bloco de trabalho."""
        pass

    @abstractmethod
    def log_phase_telemetry(self, phase_index: int, phase_name: str, records_in: int, records_out: int, duration: float) -> None:
        """Registra a telemetria detalhada de uma fase concluída."""
        pass

    @abstractmethod
    def log_work_unit_completion(self, total_links: int, remaining: int, duration: float) -> None:
        """Imprime o encerramento consolidado do bloco de trabalho."""
        pass