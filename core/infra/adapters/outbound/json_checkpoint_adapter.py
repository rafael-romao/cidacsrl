import os
import json
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone

from core.application.ports.outbound.checkpoint_port import CheckpointPort
from core.application.domain.models.tracking.work_unit import WorkUnitExecutionRecord, WorkUnitStatus

logger = logging.getLogger("Adapter: JSON Checkpoint")


class JSONCheckpointAdapter(CheckpointPort):
    def __init__(self, tracking_directory: str, project_name: str):
        self.tracking_directory = tracking_directory
        self.project_name = project_name
        self.project_path = os.path.join(self.tracking_directory, self.project_name)
        os.makedirs(self.project_path, exist_ok=True)

    def _resolve_checkpoint_path(self, job_id: str) -> str:
        job_dir = os.path.join(self.project_path, job_id)
        os.makedirs(job_dir, exist_ok=True)
        return os.path.join(job_dir, "state.json")

    def _read_state(self, file_path: str) -> Dict[str, Any]:
        if not os.path.exists(file_path):
            return {"meta": {}, "units": {}}
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                raw = json.load(f)
            except json.JSONDecodeError:
                backup_path = file_path + ".corrupt"
                os.replace(file_path, backup_path)
                logger.error(
                    f"Checkpoint corrompido em '{file_path}'. "
                    f"Arquivo movido para '{backup_path}'. Estado reiniciado."
                )
                return {"meta": {}, "units": {}}
        if "units" not in raw:
            # Fallback para formato antigo (sem envelope)
            return {"meta": {}, "units": raw}
        return raw

    def _write_state(self, file_path: str, state: Dict[str, Any]) -> None:
        tmp_path = file_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
        os.replace(tmp_path, file_path)

    def initialize_job_state(self, job_id: str, work_units: List[WorkUnitExecutionRecord]) -> None:
        file_path = self._resolve_checkpoint_path(job_id)

        if os.path.exists(file_path):
            logger.info(f"[{job_id}] - O JOB será reiniciado a partir do último estado válido.")
            logger.debug(f"[{job_id}] - Arquivo de checkpoint existente em '{file_path}'.")

            state = self._read_state(file_path)

            if not os.path.exists(file_path):
                # Arquivo estava corrompido e foi movido para backup — cria estado novo abaixo
                logger.info(f"[{job_id}] - Criando novo estado após detecção de corrupção.")
            else:
                units = state["units"]
                interrupted = [
                    uid for uid, data in units.items()
                    if data.get("status") == WorkUnitStatus.PROCESSING.value
                ]
                if interrupted:
                    for uid in interrupted:
                        units[uid]["status"] = WorkUnitStatus.PENDING.value
                        units[uid]["started_at"] = None
                    self._write_state(file_path, state)
                    logger.warning(
                        f"[{job_id}] {len(interrupted)} unidade(s) interrompidas resetadas para PENDING: {interrupted}"
                    )
                return

        logger.debug(f"[{job_id}] - Criando novo arquivo em '{file_path}'")
        state = {
            "meta": {
                "schema_version": 1,
                "job_id": job_id,
                "project_name": self.project_name,
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
            "units": {record.unit_id: record.to_dict() for record in work_units},
        }
        self._write_state(file_path, state)
        logger.info(f"[{job_id}] - Arquivo de checkpoint criado.")

    def get_pending_work_units(self, job_id: str) -> List[WorkUnitExecutionRecord]:
        file_path = self._resolve_checkpoint_path(job_id)
        units = self._read_state(file_path)["units"]
        return [
            WorkUnitExecutionRecord.from_dict(unit_id, unit_data)
            for unit_id, unit_data in units.items()
            if unit_data.get("status") != WorkUnitStatus.COMPLETED.value
        ]

    def get_all_work_units(self, job_id: str) -> List[WorkUnitExecutionRecord]:
        file_path = self._resolve_checkpoint_path(job_id)
        units = self._read_state(file_path)["units"]
        return [WorkUnitExecutionRecord.from_dict(unit_id, unit_data) for unit_id, unit_data in units.items()]

    def update_work_unit_status(
        self,
        job_id: str,
        unit_id: str,
        status: WorkUnitStatus,
        records_processed: int = 0,
        error_message: str = None,
    ) -> None:
        file_path = self._resolve_checkpoint_path(job_id)
        state = self._read_state(file_path)
        units = state["units"]

        if unit_id not in units:
            raise ValueError(
                f"[{job_id}] Tentativa de atualizar unidade inexistente no tracking: '{unit_id}'"
            )

        old_record = WorkUnitExecutionRecord.from_dict(unit_id, units[unit_id])
        timestamp_now = datetime.now(timezone.utc).isoformat()

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
            records_processed=(
                records_processed if status == WorkUnitStatus.COMPLETED else old_record.records_processed
            ),
            started_at=started_at,
            finished_at=finished_at,
            error_message=error_message,
        )

        units[unit_id] = updated_record.to_dict()
        self._write_state(file_path, state)
        logger.debug(f"[{job_id}] Unidade '{unit_id}': {status.value}.")
