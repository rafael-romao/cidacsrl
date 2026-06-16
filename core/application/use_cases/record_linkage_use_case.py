import logging
from typing import Any
from core.domain.models.linkage_specification import SequentialLinkageSpecification
from core.domain.models.tracking.work_unit import WorkUnitStatus
from core.application.services.work_unit_orchestrator import WorkUnitOrchestrator
from core.application.ports.outbound.data_persistence_port import DataPersistencePort
from core.application.ports.outbound.data_transformation_port import DataTransformationPort
from core.application.ports.outbound.get_candidates_port import GetCandidatesPort
from core.application.ports.outbound.scoring_port import ScoringPort
from core.application.ports.outbound.execution_tracking_port import ExecutionTrackingPort

logger = logging.getLogger(__name__)

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
        tracking_port: ExecutionTrackingPort
    ):
        self.orchestrator = orchestrator
        self.persistence = persistence_port
        self.transformation = transformation_port
        self.get_candidates = get_candidates_port
        self.scoring = scoring_port
        self.tracking = tracking_port

    def execute(self, specification: SequentialLinkageSpecification, job_id: str, execution_config: Any) -> None:
        project_name = specification.linkage_project_name
        logger.info(f"[{job_id}] - Linkage '{project_name}' - Fonte: '{specification.source_table}' - Índice: '{specification.target_es_index}'.")

        
        work_stream = self.orchestrator.prepare_and_route(
            table_name=specification.source_table,
            execution_config=execution_config
        )

        for payload in work_stream:
            logger.info(f"[{job_id}] - Processando unidade de trabalho: {payload.unit_id}")
            
           
            df_remaining = payload.dataframe
            total_unit_persisted = 0
            

            for phase_index, phase_context in enumerate(specification.build_blocking_phase_contexts(), start=1):
                if not phase_context.enabled:
                    continue
                
                
                remaining_count = df_remaining.count()
                logger.info(f"[{job_id}] - [{payload.unit_id}] - Fase {phase_index}: ('{phase_context.phase_name}') - Registros entrantes: {remaining_count}")
                if remaining_count == 0:
                    logger.info(f"[{job_id}] - [{payload.unit_id}] - Fase {phase_index}: ('{phase_context.phase_name}') - Sem registros remanescentes, encerrando Linkage...")
                    break
                
                
                candidates_df = self.get_candidates.get_candidates(df_remaining, phase_context)
                
                
                scored_df = self.scoring.calculate_score(candidates_df, phase_context)
                
                
                matched_pairs = self.transformation.filter_matches_by_threshold(
                    dataset=scored_df, 
                    threshold=phase_context.strong_match_score_threshold
                )
                
               
                matched_pairs.cache()
                
               
                phase_marked = self.transformation.add_phase_marker(matched_pairs, phase_context.phase_name)
                
                records_saved = self.persistence.save_phase_output(
                    df=phase_marked,
                    project_name=project_name,
                    job_id=job_id,
                    unit_id=payload.unit_id,
                    phase_name=phase_context.phase_name
                )
                
                total_unit_persisted += records_saved

                
                df_remaining = self.transformation.exclude_records(
                    primary_dataset=df_remaining,
                    records_to_exclude=matched_pairs,
                    join_key=specification.id_source_table
                )
                df_remaining.cache()           


            
            self.tracking.update_work_unit_status(
                job_id=job_id,
                unit_id=payload.unit_id,
                status=WorkUnitStatus.COMPLETED,
                records_processed=total_unit_persisted
            )
            logger.info(f"[{job_id}] - [{payload.unit_id}] - Bloco consolidado e finalizado com {total_unit_persisted} links.")