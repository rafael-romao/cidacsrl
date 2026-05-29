import os
import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_ingestion_port import DataIngestionPort
from cidacsrl_rlp.cidacsrl.infra.configs.models.storage_config import SourceStorageConfig

logger = logging.getLogger(__name__)

class SparkDataIngestionAdapter(DataIngestionPort):
    def __init__(self, spark_session: SparkSession, config: SourceStorageConfig):
        self.spark = spark_session
        self.config = config

    def _resolve_source_path(self, table_name: str) -> str:
        return os.path.join(self.config.source_data_path, table_name)

    def check_health(self, source_table: str) -> list[str]:
        errors = []
        try:            
            logger.debug(f"Verificando acesso ao caminho de origem para '{source_table}'.")
            physical_input_path = self._resolve_source_path(source_table)
            self.spark.read.format(self.config.source_data_format).load(physical_input_path).limit(1).collect()
        except Exception as e:
            errors.append(f"Erro ao acessar dados de origem em {physical_input_path}: {e}")
        return errors

    def read_source_data(self, table_name: str, **kwargs) -> DataFrame:
        physical_path = self._resolve_source_path(table_name)
        logger.debug(f"Carregando dados da origem: {physical_path}")        
        return self.spark.read.format(self.config.source_data_format).load(physical_path)

    def read_target_data(self, index_name: str, **kwargs) -> DataFrame:
        physical_path = self._resolve_source_path(index_name)
        return self.spark.read.format(self.config.source_data_format).load(physical_path)
    
    def read_specific_partition(self, table_name: str, partition_expr: str, **kwargs) -> DataFrame:        
        return self.read_source_data(table_name).filter(partition_expr)

    def get_partitioned_sample(self, table_name: str, fraction: float, seed: Optional[int] = None, **kwargs) -> DataFrame:
        return self.read_source_data(table_name).sample(withReplacement=False, fraction=fraction, seed=seed)