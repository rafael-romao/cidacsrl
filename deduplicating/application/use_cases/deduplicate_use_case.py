import logging
import time

from deduplicating.application.domain.models.deduplication_specification import DeduplicationSpecification
from deduplicating.application.ports.outbound.data_reader_port import DataReaderPort
from deduplicating.application.ports.outbound.graph_processing_port import GraphProcessingPort
from deduplicating.application.ports.outbound.data_persistence_port import DataPersistencePort

logger = logging.getLogger("UseCase: Deduplication")


class DeduplicateUseCase:
    """
    Caso de uso responsável por identificar grupos de registros duplicados
    em um dataset de pares linkados, via algoritmo de componentes conectados.

    Pipeline:
        1. Lê os pares linkados via DataReaderPort.
        2. Executa componentes conectados via GraphProcessingPort,
           retornando um DataFrame (id, cluster_id) sem expor GraphFrames.
        3. Junta o cluster_id de volta aos pares originais usando id_source_column.
        4. Persiste o resultado via DataPersistencePort.
    """

    def __init__(
        self,
        reader: DataReaderPort,
        graph_processor: GraphProcessingPort,
        persistence: DataPersistencePort,
    ):
        self.reader = reader
        self.graph_processor = graph_processor
        self.persistence = persistence

    def execute(self, spec: DeduplicationSpecification) -> None:
        logger.info(
            f"Iniciando deduplicação — "
            f"id_source='{spec.id_source_column}', "
            f"id_target='{spec.id_target_column}', "
            f"output_group_col='{spec.output_group_id_column}'."
        )
        start = time.time()

        df_pairs = self.reader.read_linked_pairs()
        logger.info("Pares linkados carregados.")

        df_clusters = self.graph_processor.find_clusters(
            df_pairs=df_pairs,
            id_source_column=spec.id_source_column,
            id_target_column=spec.id_target_column,
        )
        logger.info("Componentes conectados calculados.")

        df_result = (
            df_pairs
            .join(df_clusters, on=[df_pairs[spec.id_source_column] == df_clusters.id], how="left")
            .drop("id")
            .withColumnRenamed("cluster_id", spec.output_group_id_column)
        )

        self.persistence.save(df_result)

        logger.info(f"Deduplicação concluída em {time.time() - start:.2f}s.")
