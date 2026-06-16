from core.cidacsrl.application.ports.outbound.data_ingestion_port import DataIngestionPort
from core.cidacsrl.application.ports.outbound.data_indexing_port import DataIndexingPort
from core.cidacsrl.domain.models.indexing_specification import DatasetIndexingSpecification

class IndexDatasetUseCase:
    def __init__(self, ingestion_port: DataIngestionPort, indexing_port: DataIndexingPort):
        self.ingestion_port = ingestion_port
        self.indexing_port = indexing_port

    def execute(self, spec: DatasetIndexingSpecification) -> None:        
        source_table = spec.source_config.source_table
        
        self.indexing_port.ensure_index_with_mapping(spec)
        
        df_source = self.ingestion_port.read_all(table_name=source_table)
        
        index_columns = [col.name for col in spec.index_columns]
        df_index = df_source.select(*index_columns)
        
        self.indexing_port.index_dataframe(
            df=df_index,
            spec=spec
        )