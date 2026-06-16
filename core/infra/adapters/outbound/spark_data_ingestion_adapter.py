import os
import logging
from typing import List, Dict, Any, Optional
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame

from core.application.ports.outbound.data_ingestion_port import DataIngestionPort
from core.infra.configs.models.storage_config import SourceStorageConfig

logger = logging.getLogger(__name__)

class SparkDataIngestionAdapter(DataIngestionPort):
    def __init__(self, spark_session: SparkSession, storage_config: SourceStorageConfig):
        self.spark = spark_session
        self.storage_config = storage_config

    def _resolve_source_path(self, table_name: str) -> str:
        return os.path.join(self.storage_config.source_path, table_name)

    def check_health(self, source_table: str) -> List[str]:
        errors = []
        physical_path = self._resolve_source_path(source_table)
        try:
            if not os.path.exists(physical_path) and not physical_path.startswith("hdfs://"):
                errors.append(f"Caminho físico da tabela de origem não localizado: '{physical_path}'")
        except Exception as e:
            errors.append(f"Falha ao validar a saúde do storage de origem: {str(e)}")
        return errors

    def discover_partitions(self, table_name: str, partition_column: str) -> List[str]:
        logger.info(f"A realizar varredura dinâmica na coluna '{partition_column}' da tabela '{table_name}'...")
        try:
            path = self._resolve_source_path(table_name)
            raw_df = self.spark.read.format(self.storage_config.source_format).load(path)
            
            distinct_rows = raw_df.select(partition_column).distinct().collect()
            discovered = [str(row[partition_column]) for row in distinct_rows if row[partition_column] is not None]
            
            return sorted(discovered)
        except Exception as e:
            logger.error(f"Erro ao computar partições dinâmicas via Spark distinct: {e}")
            raise e

    def read_all(self, table_name: str) -> DataFrame:
        path = self._resolve_source_path(table_name)
        return self.spark.read.format(self.storage_config.source_format).load(path)

    def read_slice(self, table_name: str, filters: Dict[str, Any]) -> DataFrame:
        df = self.read_all(table_name)
        
        for column_name, filter_value in filters.items():
            df = df.filter(F.col(column_name) == F.lit(filter_value))
            
        return df
        
    def get_partitioned_sample(self, table_name: str, fraction: float, seed: Optional[int] = None, **kwargs) -> DataFrame:
        return self.read_source_data(table_name).sample(withReplacement=False, fraction=fraction, seed=seed)