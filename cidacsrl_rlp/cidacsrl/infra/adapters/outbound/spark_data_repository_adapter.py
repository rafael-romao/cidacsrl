from typing import Any, Optional
from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_ingestion_port import DataIngestionPort
from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_persistence_port import DataPersistencePort
from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_transformation_port import DataTransformationPort
from pyspark.sql import SparkSession, DataFrame
import os
import logging

logger = logging.getLogger(__name__)

class SparkDataRepositoryAdapter(DataIngestionPort, DataPersistencePort, DataTransformationPort):
    def __init__(self, spark_session: SparkSession, env_config: Any):
        self.spark = spark_session        
        self.source_base_path = env_config.source_data_path
        self.output_base_path = env_config.output_base_path

    def _resolve_source_path(self, table_name: str) -> str:
        return os.path.join(self.source_base_path, table_name)
    
    def _resolve_output_path(self, table_name: str) -> str:
        return os.path.join(self.output_base_path, table_name)

    def check_health(self, source_table: str, target_index: str) -> list[str]:
        errors = []
        
        # Input
        try:            
            physical_input_path = self._resolve_source_path(source_table)
            self.spark.read.format("parquet").load(physical_input_path).limit(0).schema
        except Exception as e:
            errors.append(f"Falha ao acessar o filesystem para LEITURA na tabela {source_table}: {str(e)}")
            
        # Output
        try:
            physical_output_path = os.path.join(self.output_base_path, target_index)
            test_df = self.spark.createDataFrame([("test",)], ["check"])
            test_df.write.mode("overwrite").format("parquet").save(f"{physical_output_path}/.write_test")
        except Exception as e:
            errors.append(f"Falha ao acessar o filesystem para ESCRITA no destino {target_index}: {str(e)}")
            
        return errors

    def read_data(self, table_name: str, data_format: str, **kwargs) -> DataFrame:
        physical_path = self._resolve_source_path(table_name)
        logger.debug(f"Mapeando tabela lógica '{table_name}' para caminho: {physical_path}")        
        
        return self.spark.read.format(data_format).load(physical_path)

    def read_target_data(self, index_name: str, data_format: str, **kwargs) -> DataFrame:
        physical_path = self._resolve_source_path(index_name)
        logger.debug(f"Mapeando índice ES '{index_name}' para caminho: {physical_path}")        
        
        return self.spark.read.format(data_format).load(physical_path)
    
    def read_specific_partition(self, table_name: str, partition_expr: str, data_format: str, **kwargs) -> DataFrame:        
        return self.read_data(table_name, data_format).filter(partition_expr)

    def get_partitioned_sample(self, table_name: str, fraction: float, seed: Optional[int] = None, **kwargs) -> DataFrame:
        """Gera uma amostragem estatística controlada da tabela especificada."""
        return self.read_data(table_name, "parquet").sample(withReplacement=False, fraction=fraction, seed=seed)   

    def write_data(self, data: DataFrame, table_name: str, data_format: str, **kwargs) -> None:        
        physical_path = self._resolve_output_path(table_name)
        data.write.mode("overwrite").format(data_format).save(physical_path)

    def exclude_records(self, primary_dataset: DataFrame, records_to_exclude: DataFrame, join_key: str) -> DataFrame:
        return primary_dataset.join(
            records_to_exclude,
            on=primary_dataset[join_key] == records_to_exclude[join_key],
            how="left_anti"
        )