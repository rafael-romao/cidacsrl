from typing import List

from cidacsrl.ports.indexing.telemetry_port import IndexingTelemetryPort
from cidacsrl.ports.linkage.telemetry_port import TelemetryPort


class CompositeLinkageTelemetryAdapter(TelemetryPort):

    def __init__(self, adapters: List[TelemetryPort]) -> None:
        self._adapters = adapters

    def log_job_start(self, job_id: str, project_name: str, total_units: int) -> None:
        for a in self._adapters:
            a.log_job_start(job_id, project_name, total_units)

    def log_work_unit_start(self, job_id: str, unit_id: str, pending_count: int) -> None:
        for a in self._adapters:
            a.log_work_unit_start(job_id, unit_id, pending_count)

    def log_phase_skipped(self, job_id: str, unit_id: str, phase_index: int, phase_name: str) -> None:
        for a in self._adapters:
            a.log_phase_skipped(job_id, unit_id, phase_index, phase_name)

    def log_phase_exhausted(self, job_id: str, unit_id: str, phase_index: int, phase_name: str) -> None:
        for a in self._adapters:
            a.log_phase_exhausted(job_id, unit_id, phase_index, phase_name)

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
        for a in self._adapters:
            a.log_phase_telemetry(
                job_id, unit_id, phase_index, phase_name,
                records_in, candidates_found, records_out,
                duration, search_duration, persist_duration,
            )

    def log_work_unit_completion(
        self, job_id: str, unit_id: str, total_links: int, remaining: int, duration: float
    ) -> None:
        for a in self._adapters:
            a.log_work_unit_completion(job_id, unit_id, total_links, remaining, duration)

    def log_work_unit_failure(
        self, job_id: str, unit_id: str, error_message: str, duration: float
    ) -> None:
        for a in self._adapters:
            a.log_work_unit_failure(job_id, unit_id, error_message, duration)

    def log_job_completion(self, job_id: str, total_units: int, duration: float) -> None:
        for a in self._adapters:
            a.log_job_completion(job_id, total_units, duration)


class CompositeIndexingTelemetryAdapter(IndexingTelemetryPort):

    def __init__(self, adapters: List[IndexingTelemetryPort]) -> None:
        self._adapters = adapters

    def log_indexing_start(self, source_table: str, index_name: str, column_count: int) -> None:
        for a in self._adapters:
            a.log_indexing_start(source_table, index_name, column_count)

    def log_index_ensured(self, index_name: str, duration: float) -> None:
        for a in self._adapters:
            a.log_index_ensured(index_name, duration)

    def log_indexing_completion(self, source_table: str, index_name: str, total_duration: float) -> None:
        for a in self._adapters:
            a.log_indexing_completion(source_table, index_name, total_duration)
