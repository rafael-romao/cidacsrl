import dataclasses
import json
from pathlib import Path

from core.application.ports.outbound.telemetry_port import TelemetryPort
from core.application.ports.outbound.indexing_telemetry_port import IndexingTelemetryPort
from core.infra.adapters.outbound.telemetry_events import (
    JobStartEvent,
    UnitStartEvent,
    PhaseSkippedEvent,
    PhaseExhaustedEvent,
    PhaseTelemetryEvent,
    UnitCompleteEvent,
    UnitFailureEvent,
    JobCompleteEvent,
    IndexingStartEvent,
    IndexEnsuredEvent,
    IndexingCompleteEvent,
)


class JsonlLinkageTelemetryAdapter(TelemetryPort):
    """Persists linkage telemetry events as JSON Lines for post-run analysis."""

    def __init__(self, file_path: str) -> None:
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        self._file_path = file_path

    def _append(self, event) -> None:
        with open(self._file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(dataclasses.asdict(event), ensure_ascii=False) + "\n")

    def log_job_start(self, job_id: str, project_name: str, total_units: int) -> None:
        self._append(JobStartEvent(job_id=job_id, project_name=project_name, total_units=total_units))

    def log_work_unit_start(self, job_id: str, unit_id: str, pending_count: int) -> None:
        self._append(UnitStartEvent(job_id=job_id, unit_id=unit_id, pending_count=pending_count))

    def log_phase_skipped(self, job_id: str, unit_id: str, phase_index: int, phase_name: str) -> None:
        self._append(PhaseSkippedEvent(job_id=job_id, unit_id=unit_id, phase_index=phase_index, phase_name=phase_name))

    def log_phase_exhausted(self, job_id: str, unit_id: str, phase_index: int, phase_name: str) -> None:
        self._append(PhaseExhaustedEvent(job_id=job_id, unit_id=unit_id, phase_index=phase_index, phase_name=phase_name))

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
        self._append(PhaseTelemetryEvent(
            job_id=job_id,
            unit_id=unit_id,
            phase_index=phase_index,
            phase_name=phase_name,
            records_in=records_in,
            candidates_found=candidates_found,
            records_out=records_out,
            duration_s=duration,
            search_duration_s=search_duration,
            persist_duration_s=persist_duration,
        ))

    def log_work_unit_completion(
        self, job_id: str, unit_id: str, total_links: int, remaining: int, duration: float
    ) -> None:
        self._append(UnitCompleteEvent(
            job_id=job_id, unit_id=unit_id, total_links=total_links, remaining=remaining, duration_s=duration
        ))

    def log_work_unit_failure(
        self, job_id: str, unit_id: str, error_message: str, duration: float
    ) -> None:
        self._append(UnitFailureEvent(
            job_id=job_id, unit_id=unit_id, error_message=error_message, duration_s=duration
        ))

    def log_job_completion(self, job_id: str, total_units: int, duration: float) -> None:
        self._append(JobCompleteEvent(job_id=job_id, total_units=total_units, duration_s=duration))


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
