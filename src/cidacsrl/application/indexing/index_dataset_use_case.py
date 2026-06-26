import time

from cidacsrl.domain.indexing.indexing_specification import (
    DatasetIndexingSpecification,
)
from cidacsrl.ports.indexing.data_indexing_port import DataIndexingPort
from cidacsrl.ports.indexing.telemetry_port import IndexingTelemetryPort
from cidacsrl.ports.linkage.data_ingestion_port import DataIngestionPort


class IndexDatasetUseCase:
    """Caso de uso para criação de índice Elasticsearch e ingestão de dados.

    Args:
        ingestion_port: Porta para leitura da tabela de origem.
        indexing_port: Porta para criação do índice e ingestão no ES.
        telemetry_port: Porta para registro de telemetria de indexação.
    """

    def __init__(
        self,
        ingestion_port: DataIngestionPort,
        indexing_port: DataIndexingPort,
        telemetry_port: IndexingTelemetryPort,
    ):
        self.ingestion_port = ingestion_port
        self.indexing_port = indexing_port
        self.telemetry = telemetry_port

    def execute(self, spec: DatasetIndexingSpecification) -> None:
        """Executa o pipeline de indexação: garante o índice e ingere os dados.

        Args:
            spec: Especificação completa do dataset a indexar (fonte, índice, colunas).
        """
        source_table = spec.source_config.source_table
        index_name = spec.index_config.name
        total_start = time.time()

        self.telemetry.log_indexing_start(
            source_table=source_table,
            index_name=index_name,
            column_count=len(spec.index_columns),
        )

        step_start = time.time()
        self.indexing_port.ensure_index_with_mapping(spec)
        self.telemetry.log_index_ensured(source_table=source_table, index_name=index_name, duration=time.time() - step_start)

        df_source = self.ingestion_port.read_all(table_name=source_table)
        index_columns = [col.name for col in spec.index_columns]
        df_index = df_source.select(*index_columns)

        self.indexing_port.index_dataframe(df=df_index, spec=spec)

        self.telemetry.log_indexing_completion(
            source_table=source_table,
            index_name=index_name,
            total_duration=time.time() - total_start,
        )
