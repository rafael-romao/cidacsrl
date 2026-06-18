import logging

from core.application.ports.outbound.telemetry_port import TelemetryPort

logger = logging.getLogger("Adapter: Telemetry")


class FormattedLogTelemetryAdapter(TelemetryPort):

    def log_job_start(self, job_id: str, project_name: str, total_units: int) -> None:
        logger.info("=========================================================================")
        logger.info(f" JOB '{job_id}' | PROJETO: '{project_name}' | BLOCOS: {total_units}")
        logger.info("=========================================================================")

    def log_work_unit_start(self, job_id: str, unit_id: str, pending_count: int) -> None:
        if unit_id == "global":
            logger.info("=========================================================================")
            logger.info(f"│ {job_id:<10} |             PROCESSANDO EM UM ÚNICO BLOCO             │")
            logger.info("=========================================================================")
        else:
            logger.info("┌───────────────────────────────────────────────────────────────────────┐")
            logger.info(f"│ {job_id:<10} | PENDENTES: {pending_count:<3} |  BLOCO: {unit_id:<35} │")
            logger.info("└───────────────────────────────────────────────────────────────────────┘")

    def log_phase_skipped(self, job_id: str, unit_id: str, phase_index: int, phase_name: str) -> None:
        logger.info(f"  ├── [Fase {phase_index}: {phase_name:<12}] PULADA (desabilitada)")

    def log_phase_exhausted(self, job_id: str, unit_id: str, phase_index: int, phase_name: str) -> None:
        logger.info(
            f"  ├── [Fase {phase_index}: {phase_name:<12}] ENCERRADA antecipadamente "
            f"(sem registros restantes antes de '{phase_name}')"
        )

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
        logger.info(
            f"  ├── [Fase {phase_index}: {phase_name:<12}] "
            f"Entrantes: {records_in:<5} | Candidatos: {candidates_found:<6} | Pares: {records_out:<4} | "
            f"Busca: {search_duration:.2f}s | Persist: {persist_duration:.2f}s | Total: {duration:.2f}s"
        )

    def log_work_unit_completion(
        self, job_id: str, unit_id: str, total_links: int, remaining: int, duration: float
    ) -> None:
        throughput = total_links / duration if duration > 0 else 0
        logger.info(
            f"  └── [Consolidado]   -> Total Pares Gerados: {total_links:<4} | "
            f"Registros restantes: {remaining:<4} | Tempo Total do Bloco: {duration:.2f}s | "
            f"Throughput: {throughput:.2f} pares/s\n"
        )

    def log_work_unit_failure(
        self, job_id: str, unit_id: str, error_message: str, duration: float
    ) -> None:
        logger.error(
            f"  └── [FALHA] Bloco '{unit_id}' | Duração: {duration:.2f}s | Erro: {error_message}"
        )

    def log_job_completion(self, job_id: str, total_units: int, duration: float) -> None:
        logger.info("=========================================================================")
        logger.info(
            f" TEMPO DE EXECUÇÃO DO RECORD LINKAGE: {duration:.2f}s | BLOCOS: {total_units}"
        )
        logger.info("=========================================================================")
