import logging
import time
from cidacsrl.domain.linkage.linkage_specification import SequentialLinkageSpecification
from cidacsrl.domain.linkage.tracking.work_unit import WorkUnitStatus
from core.application.services.work_unit_orchestrator import WorkUnitOrchestrator
from cidacsrl.ports.linkage.data_persistence_port import DataPersistencePort
from cidacsrl.ports.linkage.data_transformation_port import DataTransformationPort
from cidacsrl.ports.linkage.get_candidates_port import GetCandidatesPort
from cidacsrl.ports.linkage.scoring_port import ScoringPort
from cidacsrl.ports.linkage.checkpoint_port import CheckpointPort
from cidacsrl.ports.linkage.telemetry_port import TelemetryPort
from cidacsrl.config.models.execution_config import ExecutionConfig

logger = logging.getLogger("UseCase: Record Linkage")


class RecordLinkageUseCase:
    """
    Caso de Uso Principal encarregado de rodar o pipeline multifases de Linkage.
    Garante que registros já pareados com alta confiança sejam removidos (left-anti)
    para as fases subsequentes dentro de cada Work Unit.
    """
    def __init__(
        self,
        orchestrator: WorkUnitOrchestrator,
        persistence_port: DataPersistencePort,
        transformation_port: DataTransformationPort,
        get_candidates_port: GetCandidatesPort,
        scoring_port: ScoringPort,
        checkpoint_port: CheckpointPort,
        telemetry_port: TelemetryPort,
    ):
        self.orchestrator = orchestrator
        self.persistence = persistence_port
        self.transformation = transformation_port
        self.get_candidates = get_candidates_port
        self.scoring = scoring_port
        self.checkpoint = checkpoint_port
        self.telemetry = telemetry_port

    def execute(self, specification: SequentialLinkageSpecification, job_id: str, execution_config: ExecutionConfig) -> None:
        project_name = specification.linkage_project_name
        logger.info(f"[{job_id}] - Linkage '{project_name}' - Fonte: '{specification.source_table}' - Índice: '{specification.target_es_index}'.")

        partition_column = execution_config.partitioning.partition_column

        work_stream = self.orchestrator.route(
            table_name=specification.source_table,
            execution_config=execution_config
        )

        all_units = self.checkpoint.get_all_work_units(job_id)
        total_units = len(all_units)
        pending_count = total_units
        total_start_time = time.time()

        self.telemetry.log_job_start(job_id, project_name, total_units)

        for payload in work_stream:
            unit_start_time = time.time()

            pending_count -= 1
            self.telemetry.log_work_unit_start(job_id, payload.unit_id, pending_count)

            df_remaining = payload.dataframe
            total_unit_persisted = 0
            remaining_count = 0
            records_saved = 0

            try:
                for phase_index, phase_context in enumerate(specification.build_blocking_phase_contexts(), start=1):
                    if not phase_context.enabled:
                        self.telemetry.log_phase_skipped(job_id, payload.unit_id, phase_index, phase_context.phase_name)
                        continue

                    phase_start_time = time.time()
                    remaining_count = df_remaining.count()

                    if remaining_count == 0:
                        self.telemetry.log_phase_exhausted(job_id, payload.unit_id, phase_index, phase_context.phase_name)
                        break

                    # Sub-fase: Busca ES — materializa via .count() para capturar duração real da rede
                    search_start = time.time()
                    candidates_df = self.get_candidates.get_candidates(df_remaining, phase_context)
                    candidates_df.cache()
                    candidates_found = candidates_df.count()
                    search_duration = time.time() - search_start

                    # Sub-fase: Score + Filtro (lazy) — materializado junto com a persistência
                    scored_df = self.scoring.calculate_score(candidates_df, phase_context)
                    matched_pairs = self.transformation.filter_matches_by_threshold(
                        dataset=scored_df,
                        threshold=phase_context.strong_match_score_threshold
                    )
                    matched_pairs.cache()

                    phase_marked = self.transformation.add_phase_marker(matched_pairs, phase_context.phase_name)

                    # Sub-fase: Persistência — materializa scoring+filtro+escrita
                    persist_start = time.time()
                    records_saved = self.persistence.save_phase_output(
                        df=phase_marked,
                        project_name=project_name,
                        phase_name=phase_context.phase_name,
                        partition_column=partition_column,
                    )
                    persist_duration = time.time() - persist_start

                    total_unit_persisted += records_saved

                    df_remaining = self.transformation.exclude_records(
                        primary_dataset=df_remaining,
                        records_to_exclude=matched_pairs,
                        join_key=specification.id_source_table
                    )
                    df_remaining.cache()

                    self.telemetry.log_phase_telemetry(
                        job_id=job_id,
                        unit_id=payload.unit_id,
                        phase_index=phase_index,
                        phase_name=phase_context.phase_name,
                        records_in=remaining_count,
                        candidates_found=candidates_found,
                        records_out=records_saved,
                        duration=time.time() - phase_start_time,
                        search_duration=search_duration,
                        persist_duration=persist_duration,
                    )

                self.checkpoint.update_work_unit_status(
                    job_id=job_id,
                    unit_id=payload.unit_id,
                    status=WorkUnitStatus.COMPLETED,
                    records_processed=total_unit_persisted,
                )
                self.telemetry.log_work_unit_completion(
                    job_id=job_id,
                    unit_id=payload.unit_id,
                    total_links=total_unit_persisted,
                    remaining=remaining_count - records_saved,
                    duration=time.time() - unit_start_time,
                )

            except Exception as e:
                self.checkpoint.update_work_unit_status(
                    job_id=job_id,
                    unit_id=payload.unit_id,
                    status=WorkUnitStatus.FAILED,
                    error_message=str(e),
                )
                self.telemetry.log_work_unit_failure(
                    job_id=job_id,
                    unit_id=payload.unit_id,
                    error_message=str(e),
                    duration=time.time() - unit_start_time,
                )
                raise

        self.telemetry.log_job_completion(job_id, total_units, time.time() - total_start_time)
