import logging
from typing import Any

from deduplicating.application.ports.outbound.data_reader_port import DataReaderPort
from deduplicating.infra.configs.models.deduplicate_workflow_config import DeduplicateStorageConfig

logger = logging.getLogger("Adapter: SparkDataReader")


class SparkDataReaderAdapter(DataReaderPort):
    def __init__(self, spark: Any, storage: DeduplicateStorageConfig):
        self._spark = spark
        self._storage = storage

    def read_linked_pairs(self) -> Any:
        logger.info(
            f"Lendo pares linkados de '{self._storage.source_path}' "
            f"(formato: {self._storage.source_format})."
        )
        df = self._spark.read.format(self._storage.source_format).load(self._storage.source_path)
        logger.info(f"Pares carregados: {df.count():,} registros.")
        return df
