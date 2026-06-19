import logging
from typing import Any

from deduplicating.application.ports.outbound.data_persistence_port import DataPersistencePort
from cidacsrl.config.models.storage_config import OutputStorageConfig

logger = logging.getLogger("Adapter: SparkDataPersistence")


class SparkDataPersistenceAdapter(DataPersistencePort):
    def __init__(self, storage: OutputStorageConfig):
        self._storage = storage

    def save(self, df: Any) -> None:
        logger.info(
            f"Persistindo resultado em '{self._storage.output_path}' "
            f"(formato: {self._storage.output_format})."
        )
        df.write.mode("overwrite").format(self._storage.output_format).save(self._storage.output_path)
        logger.info("Resultado da deduplicação persistido com sucesso.")
