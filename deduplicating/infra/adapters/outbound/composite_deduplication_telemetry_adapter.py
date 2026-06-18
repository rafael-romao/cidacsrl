from typing import List

from deduplicating.application.ports.outbound.deduplication_telemetry_port import DeduplicationTelemetryPort


class CompositeDeduplicationTelemetryAdapter(DeduplicationTelemetryPort):

    def __init__(self, adapters: List[DeduplicationTelemetryPort]) -> None:
        self._adapters = adapters

    def log_deduplication_start(self, id_source: str, id_target: str, output_col: str) -> None:
        for a in self._adapters:
            a.log_deduplication_start(id_source, id_target, output_col)

    def log_pairs_loaded(self, duration: float) -> None:
        for a in self._adapters:
            a.log_pairs_loaded(duration)

    def log_clusters_found(self, duration: float) -> None:
        for a in self._adapters:
            a.log_clusters_found(duration)

    def log_deduplication_completion(self, total_duration: float) -> None:
        for a in self._adapters:
            a.log_deduplication_completion(total_duration)
