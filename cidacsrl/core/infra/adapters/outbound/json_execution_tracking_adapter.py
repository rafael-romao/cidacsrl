import os
import json
import logging
from typing import List, Dict, Any
from datetime import datetime

from cidacsrl.cidacsrl.application.ports.outbound.execution_tracking_port import ExecutionTrackingPort
from cidacsrl.cidacsrl.domain.models.tracking.work_unit import WorkUnitExecutionRecord, WorkUnitStatus

logger = logging.getLogger(__name__)

class JSONExecutionTrackingAdapter(ExecutionTrackingPort):
    def __init__(self, tracking_directory: str):
        self.tracking_directory = tracking_directory
        if self.tracking_directory:
            os.makedirs(self.tracking_directory, exist_ok=True)

    def _resolve_checkpoint_path(self, job_id: str) -> str:
        return os.path.join(self.tracking_directory, f"job_{job_id}_state.json")

    def _read_raw_file(self, file_path: str) -> Dict[str, Any]:
        if not os.path.exists(file_path):
            return {}
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"Arquivo de checkpoint corrompido detectado em {file_path}. ")
                return {}

    def _write_raw_file(self, file_path: str, data: Dict[str, Any]) -> None:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

    def initialize_job_state(self, job_id: str, work_units: List[WorkUnitExecutionRecord]) -> None:
        file_path = self._resolve_checkpoint_path(job_id)
        
        if os.path.exists(file_path):
            logger.info(f"[{job_id}] - Arquivo de checkpoint existente localizado em '{file_path}'. Processo marcado para reinício a partir do estado salvo.")
            return

        logger.info(f"[{job_id}] - Criando novo arquivo  '{file_path}'...")
        initial_state = {record.unit_id: record.to_dict() for record in work_units}
        self._write_raw_file(file_path, initial_state)

    def get_pending_work_units(self, job_id: str) -> List[WorkUnitExecutionRecord]:
        file_path = self._resolve_checkpoint_path(job_id)
        raw_state = self._read_raw_file(file_path)

        pending_records: List[WorkUnitExecutionRecord] = []
        for unit_data in raw_state.values():
            record = WorkUnitExecutionRecord.from_dict(unit_data)
            if record.status != WorkUnitStatus.COMPLETED:
                pending_records.append(record)

        return pending_records

    def update_work_unit_status(
        self, 
        job_id: str, 
        unit_id: str, 
        status: WorkUnitStatus, 
        records_processed: int = 0,
        error_message: str = None
    ) -> None:
        file_path = self._resolve_checkpoint_path(job_id)
        current_state = self._read_raw_file(file_path)

        if unit_id not in current_state:
            raise ValueError(f"[{job_id}] Tentativa de atualizar unidade inexistente no tracking: '{unit_id}'")

        old_record = WorkUnitExecutionRecord.from_dict(current_state[unit_id])
        timestamp_now = datetime.now().isoformat()

        
        started_at = old_record.started_at
        finished_at = old_record.finished_at

        if status == WorkUnitStatus.PROCESSING:
            started_at = timestamp_now
            finished_at = None
        elif status in (WorkUnitStatus.COMPLETED, WorkUnitStatus.FAILED):
            finished_at = timestamp_now

        updated_record = WorkUnitExecutionRecord(
            unit_id=old_record.unit_id,
            filters=old_record.filters,
            status=status,
            records_processed=records_processed if status == WorkUnitStatus.COMPLETED else old_record.records_processed,
            started_at=started_at,
            finished_at=finished_at,
            error_message=error_message
        )

        current_state[unit_id] = updated_record.to_dict()
        self._write_raw_file(file_path, current_state)
        logger.debug(f"[{job_id}] Unidade '{unit_id}' transicionada com sucesso para {status.value}.")