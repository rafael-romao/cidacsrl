import logging
from pathlib import Path
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from core.application.ports.outbound.data_persistence_port import DataPersistencePort
from core.infra.configs.models.storage_config import OutputStorageConfig

logger = logging.getLogger("Adapter: Persistence")

class SparkDataPersistenceAdapter(DataPersistencePort):
    def __init__(self, output_config: OutputStorageConfig):
        self.config = output_config

    def save_phase_output(
        self,
        df: DataFrame,
        project_name: str,
        phase_name: str,
        partition_column: Optional[str] = None,
    ) -> int:
        base_path = Path(self.config.output_path) / project_name

        logger.info(f"Escrevendo resultados da fase: '{phase_name}'")
        logger.debug(f"Escrevendo resultados da fase '{phase_name}' em: {str(base_path)}")

        df.cache()
        try:
            partition_cols = ["phase_match"]
            if partition_column:
                actual_col = partition_column if partition_column in df.columns else f"source_{partition_column}"
                partition_cols.append(actual_col)

            writer = (
                df.write.format(self.config.output_format)
                .mode("overwrite")
                .option("partitionOverwriteMode", "dynamic")
                .partitionBy(*partition_cols)
            )
            writer.save(str(base_path))
            return df.count()
        finally:
            df.unpersist()
