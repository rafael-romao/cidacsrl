import os
import logging
from typing import List, Any
from pyspark.sql import SparkSession, DataFrame

from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_persistence_port import DataPersistencePort
from cidacsrl_rlp.cidacsrl.infra.configs.models.storage_config import OutputStorageConfig

logger = logging.getLogger(__name__)

class SparkDataPersistenceAdapter(DataPersistencePort):
    def __init__(self, spark_session: SparkSession, config: OutputStorageConfig):
        self.spark = spark_session
        self.config = config

    def _resolve_output_path(self, unit_id: str) -> str:
        return os.path.join(self.config.output_path, f"unit_{unit_id}")

    def save_linkage_output(self, phase_outputs: List[DataFrame], unit_id: str) -> int:
        if not phase_outputs:
            return 0

        logger.info(f"Consolidando {len(phase_outputs)} fatias de fases para a unidade '{unit_id}' via unionByName...")
        
        consolidated_df = phase_outputs[0]
        for next_df in phase_outputs[1:]:
            consolidated_df = consolidated_df.unionByName(next_df)
            
        physical_path = self._resolve_output_path(unit_id)
        
        consolidated_df = consolidated_df.cache()
        
        try:
            consolidated_df.write.format(self.config.output_format).mode("overwrite").save(physical_path)
            
            total_records = consolidated_df.count()
            logger.info(f"Persistência finalizada com sucesso em: {physical_path} | Total: {total_records}")
            
            return total_records
            
        finally:
            consolidated_df.unpersist()