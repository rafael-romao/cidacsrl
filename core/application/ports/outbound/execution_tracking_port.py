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
    def update_work_unit_status(
        self, 
        job_id: str, 
        unit_id: str, 
        status: WorkUnitStatus, 
        records_processed: int = 0,
        error_message: str = None
    ) -> None:        
        pass