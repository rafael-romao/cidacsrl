import os
import logging
from pyspark.sql import SparkSession, DataFrame
from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_persistence_port import DataPersistencePort
from cidacsrl_rlp.cidacsrl.infra.configs.models.storage_config import OutputStorageConfig

logger = logging.getLogger(__name__)

class SparkDataPersistenceAdapter(DataPersistencePort):
    def __init__(self, spark_session: SparkSession, config: OutputStorageConfig):
        self.spark = spark_session
        self.config = config

    def _resolve_output_path(self, table_name: str) -> str:
        return os.path.join(self.config.output_path, table_name)

    def write_data(self, data: DataFrame, output_folder: str, **kwargs) -> None:        
        physical_path = self._resolve_output_path(output_folder)
        logger.debug(f"Persistindo dados em: {physical_path}")
        data.write.mode("overwrite").format(self.config.output_format).save(physical_path)