import dataclasses
import json
from pathlib import Path

from core.application.ports.outbound.telemetry_port import TelemetryPort
from core.application.ports.outbound.indexing_telemetry_port import IndexingTelemetryPort
from core.infra.adapters.outbound.telemetry_events import (
    PhaseRecord,
    UnitRecord,
    JobRecord,
    _ts,
    IndexingStartEvent,
    IndexEnsuredEvent,
    IndexingCompleteEvent,
)


class JsonlLinkageTelemetryAdapter(TelemetryPort):
    """Persists linkage telemetry as three JSONL files: phases, units, and job."""

    def __init__(self, phases_path: str, units_path: str, job_path: str) -> None:
        for path in (phases_path, units_path, job_path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._phases_path = phases_path
        self._units_path = units_path
        self._job_path = job_path
        self._project_name: str = ""
        self._job_started_at: str = ""
        self._unit_started_at: dict[str, str] = {}

    def _append(self, path: str, record) -> None:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(dataclasses.asdict(record), ensure_ascii=False) + "\n")

    def log_job_start(self, job_id: str, project_name: str, total_units: int) -> None:
        self._project_name = project_name
        self._job_started_at = _ts()

    def log_work_unit_start(self, job_id: str, unit_id: str, pending_count: int) -> None:
        self._unit_started_at[unit_id] = _ts()

    def log_phase_skipped(self, job_id: str, unit_id: str, phase_index: int, phase_name: str) -> None:
        self._append(self._phases_path, PhaseRecord(
            job_id=job_id, project_name=self._project_name,
            unit_id=unit_id, phase_index=phase_index, phase_name=phase_name,
            status="skipped",
        ))

    def log_phase_exhausted(self, job_id: str, unit_id: str, phase_index: int, phase_name: str) -> None:
        self._append(self._phases_path, PhaseRecord(
            job_id=job_id, project_name=self._project_name,
            unit_id=unit_id, phase_index=phase_index, phase_name=phase_name,
            status="exhausted",
        ))

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
        self._append(self._phases_path, PhaseRecord(
            job_id=job_id, project_name=self._project_name,
            unit_id=unit_id, phase_index=phase_index, phase_name=phase_name,
            status="completed",
            records_in=records_in, candidates_found=candidates_found, records_out=records_out,
            duration_s=duration, search_duration_s=search_duration, persist_duration_s=persist_duration,
        ))

    def log_work_unit_completion(
        self, job_id: str, unit_id: str, total_links: int, remaining: int, duration: float
    ) -> None:
        self._append(self._units_path, UnitRecord(
            job_id=job_id, project_name=self._project_name,
            unit_id=unit_id,
            started_at=self._unit_started_at.pop(unit_id, ""),
            completed_at=_ts(),
            status="completed",
            duration_s=duration,
            total_links=total_links, remaining=remaining,
        ))

    def log_work_unit_failure(
        self, job_id: str, unit_id: str, error_message: str, duration: float
    ) -> None:
        self._append(self._units_path, UnitRecord(
            job_id=job_id, project_name=self._project_name,
            unit_id=unit_id,
            started_at=self._unit_started_at.pop(unit_id, ""),
            completed_at=_ts(),
            status="failed",
            duration_s=duration,
        ))

    def log_job_completion(self, job_id: str, total_units: int, duration: float) -> None:
        self._append(self._job_path, JobRecord(
            job_id=job_id, project_name=self._project_name,
            total_units=total_units,
            started_at=self._job_started_at,
            completed_at=_ts(),
            duration_s=duration,
        ))


class JsonlIndexingTelemetryAdapter(IndexingTelemetryPort):
    """Persists indexing telemetry events as JSON Lines for post-run analysis."""

    def __init__(self, file_path: str) -> None:
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        self._file_path = file_path

    def _append(self, event) -> None:
        with open(self._file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(dataclasses.asdict(event), ensure_ascii=False) + "\n")

    def log_indexing_start(self, source_table: str, index_name: str, column_count: int) -> None:
        self._append(IndexingStartEvent(source_table=source_table, index_name=index_name, column_count=column_count))

    def log_index_ensured(self, index_name: str, duration: float) -> None:
        self._append(IndexEnsuredEvent(index_name=index_name, duration_s=duration))

    def log_indexing_completion(self, source_table: str, index_name: str, total_duration: float) -> None:
        self._append(IndexingCompleteEvent(
            source_table=source_table, index_name=index_name, total_duration_s=total_duration
        ))
