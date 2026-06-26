import logging
import os
from typing import Any, Dict, List, Optional, Set

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from cidacsrl.config.models.storage_config import SourceStorageConfig
from cidacsrl.ports.linkage.data_ingestion_port import DataIngestionPort

logger = logging.getLogger("Adapter: SparkDataIngestionAdapter")

class SparkDataIngestionAdapter(DataIngestionPort):
    """Adapter de ingestão de dados via Spark."""

    def __init__(self, spark_session: SparkSession, storage_config: SourceStorageConfig):
        self.spark = spark_session
        self.storage_config = storage_config
        self._partition_cache: Dict[str, List[str]] = {}

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

    def validate_source_schema(self, table_name: str, required_columns: Set[str]) -> None:
        path = self._resolve_source_path(table_name)
        available = set(self.spark.read.format(self.storage_config.source_format).load(path).schema.fieldNames())
        missing = required_columns - available
        if missing:
            raise ValueError(
                f"Colunas ausentes na tabela fonte '{table_name}': {sorted(missing)}. "
                f"Colunas disponíveis: {sorted(available)}"
            )
        logger.info(f"Schema da tabela '{table_name}' validado com sucesso. Colunas requeridas encontradas: {sorted(required_columns)}")

    def discover_partitions(self, table_name: str, partition_column: str) -> List[str]:
        cache_key = f"{table_name}:{partition_column}"
        if cache_key in self._partition_cache:
            logger.debug(f"Partições de '{table_name}/{partition_column}' recuperadas do cache.")
            return self._partition_cache[cache_key]

        logger.info(f"Varrendo a coluna '{partition_column}' da tabela '{table_name}' para encontrar partições distintas.")
        try:
            path = self._resolve_source_path(table_name)
            schema = self.spark.read.format(self.storage_config.source_format).load(path).schema

            if partition_column not in schema.fieldNames():
                raise ValueError(
                    f"Coluna de particionamento '{partition_column}' não encontrada na tabela '{table_name}'. "
                    f"Colunas disponíveis: {schema.fieldNames()}"
                )

            distinct_rows = (
                self.spark.read.format(self.storage_config.source_format)
                .load(path)
                .select(partition_column)
                .distinct()
                .collect()
            )
            discovered = sorted(
                str(row[partition_column])
                for row in distinct_rows
                if row[partition_column] is not None
            )

            logger.info(f"[{table_name}] {len(discovered)} partições detectadas em '{partition_column}'.")
            self._partition_cache[cache_key] = discovered
            return discovered
        except Exception as e:
            logger.error(f"Erro ao computar partições dinâmicas via Spark distinct: {e}")
            raise

    def read_all(self, table_name: str) -> DataFrame:
        path = self._resolve_source_path(table_name)
        return self.spark.read.format(self.storage_config.source_format).load(path)

    def read_slice(self, table_name: str, filters: Dict[str, Any]) -> DataFrame:
        df = self.read_all(table_name)
        
        for column_name, filter_value in filters.items():
            df = df.filter(F.col(column_name) == F.lit(filter_value))
            
        return df
        
    def get_partitioned_sample(self, table_name: str, fraction: float, seed: Optional[int] = None, **kwargs) -> DataFrame:
        """Retorna uma amostra aleatória da tabela de origem.

        Args:
            table_name: Nome da tabela a amostrar.
            fraction: Fração dos dados a retornar (0.0–1.0).
            seed: Semente para reprodutibilidade. Defaults to None.

        Returns:
            DataFrame com a fração amostrada da tabela.
        """
        return self.read_source_data(table_name).sample(withReplacement=False, fraction=fraction, seed=seed)