from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_ingestion_port import DataIngestionPort
from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_indexing_port import DataIndexingPort
from cidacsrl_rlp.cidacsrl.domain.models.indexing_specification import DatasetIndexingSpecification

class IndexDatasetUseCase:
    def __init__(self, ingestion_port: DataIngestionPort, indexing_port: DataIndexingPort):
        self.ingestion_port = ingestion_port
        self.indexing_port = indexing_port

    def execute(self, source_table: str, spec: DatasetIndexingSpecification, id_field: str) -> None:
        index_name = spec.index_config.name
        
        self.indexing_port.ensure_index_with_mapping(index_name, spec)
        
        df_source = self.ingestion_port.read_source_data(table_name=source_table)
        
        colunas_para_indexar = [col.name for col in spec.columns]
        df_filtrado = df_source.select(*colunas_para_indexar)
        
        self.indexing_port.index_dataframe(
            df=df_filtrado,
            index_name=index_name,
            id_field=id_field
        )