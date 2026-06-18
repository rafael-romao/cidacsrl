import logging

from deduplicating.application.ports.outbound.deduplication_telemetry_port import DeduplicationTelemetryPort

logger = logging.getLogger("Adapter: Deduplication Telemetry")


class FormattedLogDeduplicationTelemetryAdapter(DeduplicationTelemetryPort):

    def log_deduplication_start(self, id_source: str, id_target: str, output_col: str) -> None:
        logger.info("=========================================================================")
        logger.info(f" DEDUPLICAÇÃO | {id_source} ↔ {id_target} → grupo: '{output_col}'")
        logger.info("=========================================================================")

    def log_pairs_loaded(self, duration: float) -> None:
        logger.info(f"  ├── Pares linkados carregados em {duration:.2f}s ({duration/60:.2f} min)")

    def log_clusters_found(self, duration: float) -> None:
        logger.info(f"  ├── Componentes conectados calculados em {duration:.2f}s ({duration/60:.2f} min)")

    def log_deduplication_completion(self, total_duration: float) -> None:
        logger.info("=========================================================================")
        logger.info(f" DEDUPLICAÇÃO CONCLUÍDA | TEMPO: {total_duration:.2f}s ({total_duration/60:.2f} min)")
        logger.info("=========================================================================")
