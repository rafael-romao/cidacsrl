from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_ingestion_port import DataIngestionPort
from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_persistence_port import DataPersistencePort
from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_transformation_port import DataTransformationPort
from cidacsrl_rlp.cidacsrl.application.ports.outbound.get_candidates_port import GetCandidatesPort
from cidacsrl_rlp.cidacsrl.application.ports.outbound.scoring_port import ScoringPort
from cidacsrl_rlp.cidacsrl.domain.models.linkage_specification import SequentialLinkageSpecification
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import logging

logger = logging.getLogger(__name__)


class RunSequentialLinkageUseCase:
    def __init__(self, 
                 ingestion_port: DataIngestionPort,
                 persistence_port: DataPersistencePort,
                 transformation_port: DataTransformationPort,
                 get_candidates_port: GetCandidatesPort, 
                 scoring_port: ScoringPort):        
        
        # Data Ports
        self.ingestion_port = ingestion_port
        self.persistence_port = persistence_port
        self.transformation_port = transformation_port
        
        # Search & Scoring Ports
        self.get_candidates = get_candidates_port
        self.scoring = scoring_port
    
    def execute(self, config: SequentialLinkageSpecification) -> DataFrame:
        logger.info("Iniciando a execução do fluxo sequencial de Record Linkage.")
        
        
        df_source = self.ingestion_port.read_source_data(table_name=config.source_table)
        
        df_remaining = df_source
        df_remaining.cache()
        
        df_scored_pairs = None
        
        for phase_index, phase_context in enumerate(config.build_blocking_phase_contexts(), start=1):
            if not phase_context.enabled:
                logger.info(f"Fase {phase_index} ({phase_context.phase_name}) desabilitada. Pulando.")
                continue
                
            logger.info(f"Executando Fase {phase_index}: {phase_context.phase_name}")
            
            if phase_index > 1 and df_scored_pairs is not None:
                logger.info(f"Removendo candidatos já encontrados nas fases anteriores para a Fase {phase_index}.")
                
                join_key = config.id_source_table
                
                df_remaining = self.transformation_port.exclude_records(
                    primary_dataset=df_remaining,
                    records_to_exclude=df_scored_pairs,
                    join_key=join_key
                )
                df_remaining.cache()
            
            # Search Block
            df_candidates = self.get_candidates.get_candidates(df_remaining, phase_context)

            # Phase information
            df_candidates = df_candidates.withColumn("phase_match", F.lit(phase_context.phase_name))
            
            # Scoring Block
            df_scored_pairs = self.scoring.calculate_score(df_candidates, phase_context)
            
            # Intermidiate Persistence
            logger.info(f"Persistindo resultados intermediários da Fase {phase_index} com {df_scored_pairs.count()} pares pontuados.")
            phase_output_folder = f"phase_{phase_index}_{phase_context.phase_name}"
            self.persistence_port.write_data(
                data=df_scored_pairs,
                output_folder=phase_output_folder,
                
            )
            
        logger.info("Todas as fases do fluxo sequencial foram executadas e persistidas.")
        return df_scored_pairs
    