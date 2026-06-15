import logging
from pathlib import Path
from pyspark.sql import DataFrame

from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_persistence_port import DataPersistencePort
from cidacsrl_rlp.cidacsrl.infra.configs.models.storage_config import OutputStorageConfig

logger = logging.getLogger(__name__)

class SparkDataPersistenceAdapter(DataPersistencePort):
    def __init__(self, output_config: OutputStorageConfig):
        self.config = output_config 

    def save_phase_output(
        self, 
        df: DataFrame, 
        project_name: str, 
        job_id: str, 
        unit_id: str, 
        phase_name: str
    ) -> int:
        base_path = Path(self.config.output_path)
        absolute_target_path = base_path / project_name / job_id / unit_id / phase_name
        
        target_path_str = str(absolute_target_path)
        logger.info(f"Gravando fase '{phase_name}' de linkage em: {target_path_str}")
        
        df.write.format(self.config.output_format).mode("overwrite").save(target_path_str)
        return df.count()