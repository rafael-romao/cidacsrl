import dataclasses
import json
from pathlib import Path

from deduplicating.application.ports.outbound.deduplication_telemetry_port import DeduplicationTelemetryPort
from deduplicating.infra.adapters.outbound.deduplication_telemetry_events import (
    DeduplicationStartEvent,
    PairsLoadedEvent,
    ClustersFoundEvent,
    DeduplicationCompleteEvent,
)


class JsonlDeduplicationTelemetryAdapter(DeduplicationTelemetryPort):
    """Persists deduplication telemetry events as JSON Lines for post-run analysis."""

    def __init__(self, file_path: str, run_id: str) -> None:
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        self._file_path = file_path
        self._run_id = run_id

    def _append(self, event) -> None:
        with open(self._file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(dataclasses.asdict(event), ensure_ascii=False) + "\n")

    def log_deduplication_start(self, id_source: str, id_target: str, output_col: str) -> None:
        self._append(DeduplicationStartEvent(
            run_id=self._run_id,
            id_source=id_source,
            id_target=id_target,
            output_col=output_col,
        ))

    def log_pairs_loaded(self, duration: float) -> None:
        self._append(PairsLoadedEvent(run_id=self._run_id, duration_s=duration))

    def log_clusters_found(self, duration: float) -> None:
        self._append(ClustersFoundEvent(run_id=self._run_id, duration_s=duration))

    def log_deduplication_completion(self, total_duration: float) -> None:
        self._append(DeduplicationCompleteEvent(run_id=self._run_id, total_duration_s=total_duration))
