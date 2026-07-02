import logging

from cidacsrl.ports.indexing.telemetry_port import IndexingTelemetryPort
from cidacsrl.ports.linkage.telemetry_port import TelemetryPort

logger = logging.getLogger("Adapter: Telemetry")

_SEP_W = 73
_SEP = "=" * _SEP_W
_BOX_W = 56  # inner width between │ and │


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", ".")


class FormattedLogTelemetryAdapter(TelemetryPort, IndexingTelemetryPort):

    def log_job_start(self, job_id: str, project_name: str, total_units: int) -> None:
        content = f"JOB '{job_id}' | PROJETO: '{project_name}' | BLOCOS: {_fmt(total_units)}"
        logger.info(_SEP)
        logger.info(content.center(_SEP_W))
        logger.info(_SEP)

    def log_work_unit_start(self, job_id: str, unit_id: str, pending_count: int) -> None:
        border = "─" * _BOX_W
        logger.info(f"┌{border}┐")
        logger.info(f"│{job_id.center(_BOX_W)}│")
        if unit_id == "global":
            logger.info(f"│{'PROCESSANDO EM UM ÚNICO BLOCO'.center(_BOX_W)}│")
        else:
            left_w = _BOX_W // 2 - 1
            right_w = _BOX_W - left_w - 1
            left_col = f"PENDENTES: {_fmt(pending_count)}".center(left_w)
            right_col = f"BLOCO: {unit_id}".center(right_w)
            logger.info(f"│{left_col}│{right_col}│")
        logger.info(f"└{border}┘")

    def log_phase_skipped(self, job_id: str, unit_id: str, phase_index: int, phase_name: str) -> None:
        logger.info(f"  ├── [{unit_id} | Fase {phase_index}: {phase_name:<12}] PULADA (desabilitada)")

    def log_phase_exhausted(self, job_id: str, unit_id: str, phase_index: int, phase_name: str) -> None:
        logger.info(
            f"  ├── [{unit_id} | Fase {phase_index}: {phase_name:<12}] "
            f"ENCERRADA antecipadamente (sem registros restantes)"
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
        header = f"[{unit_id} | Fase {phase_index}: {phase_name:<12}]"
        indent = " " * len(header)
        logger.info(
            f"  ├── {header} "
            f"Entrantes: {_fmt(records_in):<7} | Candidatos: {_fmt(candidates_found):<9} | Pares: {_fmt(records_out):<7}"
        )
        logger.info(
            f"  │   {indent} "
            f"Busca: {search_duration:.2f}s ({search_duration/60:.2f} min) | "
            f"Persist: {persist_duration:.2f}s ({persist_duration/60:.2f} min) | "
            f"Total: {duration:.2f}s ({duration/60:.2f} min)"
        )

    def log_work_unit_completion(
        self, job_id: str, unit_id: str, total_links: int, remaining: int, duration: float
    ) -> None:
        throughput = total_links / duration if duration > 0 else 0
        throughput_str = f"{throughput:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
        logger.info(
            f"  └── [{unit_id} | Consolidado] "
            f"Total Pares: {_fmt(total_links):<7} | Restantes: {_fmt(remaining):<7}"
        )
        logger.info(
            f"       Tempo: {duration:.2f}s ({duration/60:.2f} min) | Throughput: {throughput_str} pares/s\n"
        )

    def log_work_unit_failure(
        self, job_id: str, unit_id: str, error_message: str, duration: float
    ) -> None:
        logger.error(
            f"  └── [{unit_id} | FALHA] Duração: {duration:.2f}s ({duration/60:.2f} min) | Erro: {error_message}"
        )

    def log_job_completion(self, job_id: str, total_units: int, duration: float) -> None:
        content = f"JOB '{job_id}' CONCLUÍDO | BLOCOS: {_fmt(total_units)} | TEMPO: {duration:.2f}s ({duration/60:.2f} min)"
        logger.info(_SEP)
        logger.info(content.center(_SEP_W))
        logger.info(_SEP)

    # ── IndexingTelemetryPort ────────────────────────────────────────────────

    def log_indexing_start(self, source_table: str, index_name: str, column_count: int) -> None:
        content = f"INDEXAÇÃO | Tabela: '{source_table}' → Índice: '{index_name}' | Colunas: {_fmt(column_count)}"
        logger.info(_SEP)
        logger.info(content.center(_SEP_W))
        logger.info(_SEP)

    def log_index_ensured(self, source_table: str, index_name: str, duration: float) -> None:
        logger.info(f"  ├── Índice '{index_name}' pronto em {duration:.2f}s ({duration/60:.2f} min)")

    def log_indexing_completion(self, source_table: str, index_name: str, total_duration: float) -> None:
        content = f"INDEXAÇÃO CONCLUÍDA | '{source_table}' → '{index_name}' | TEMPO: {total_duration:.2f}s ({total_duration/60:.2f} min)"
        logger.info(_SEP)
        logger.info(content.center(_SEP_W))
        logger.info(_SEP)
