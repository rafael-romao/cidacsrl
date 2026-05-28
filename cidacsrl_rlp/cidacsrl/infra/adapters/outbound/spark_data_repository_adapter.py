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
        self.env_config = env_config

    def _resolve_source_path(self, table_name: str) -> str:
        return os.path.join(self.env_config.source_data_path, table_name)
    
    def _resolve_output_path(self, table_name: str) -> str:
        return os.path.join(self.env_config.output_data_path, table_name)

    def check_health(self, source_table: str, target_index: str) -> list[str]:
        errors = []
        
        # Input
        try:            
            logger.debug(f"Verificando acesso ao caminho de origem para a tabela '{source_table}' usando formato '{self.env_config.source_data_format}'.")
            physical_input_path = self._resolve_source_path(source_table)
            self.spark.read.format(self.env_config.source_data_format).load(physical_input_path).limit(0).schema
        except Exception as e:
            errors.append(f"Falha ao acessar o caminho {physical_input_path}: {str(e)}")
            
        # Output
        try:
            logger.debug(f"Verificando acesso ao caminho de destino'{target_index}' usando formato '{self.env_config.output_data_format}'.")
            physical_output_path = os.path.join(self.env_config.output_data_path, target_index)
            test_df = self.spark.createDataFrame([("test",)], ["check"])
            test_df.write.mode("overwrite").format(self.env_config.output_data_format).save(f"{physical_output_path}/.write_test")
        except Exception as e:
            errors.append(f"Falha ao acessar o caminho {physical_output_path} para ESCRITA: {str(e)}")
            
        return errors
    
    def read_source_data(self, table_name: str, **kwargs) -> DataFrame:
        # 1. Resolve o caminho base físico usando a config de ambiente
        physical_path = self._resolve_source_path(table_name)
        
        # 2. Executa a leitura nativa usando o formato vindo da infraestrutura
        df = self.spark.read.format(self.env_config.source_data_format).load(physical_path)
        
        # 3. Aplica o particionamento físico (Ex: ler apenas UF='BA') de forma transparente
        if self.env_config.partitioning:
            part = self.env_config.partitioning
            if part.has_filters:
                # Exemplo de conversão: uf IN ('BA', 'SP')
                partitions_str = ", ".join(f"'{p}'" for p in part.filter_partitions)
                expr = f"{part.partition_column} IN ({partitions_str})"
                df = df.filter(expr)
                logger.debug(f"Aplicando filtro de partição: {expr}")

        # 4. Aplica amostragem se estiver configurada no ambiente        
        if self.env_config.sample_fraction:
            df = df.sample(
                withReplacement=False, 
                fraction=self.env_config.sample_fraction, 
                seed=self.env_config.sample_seed
            )
            logger.debug(f"Aplicando amostragem: fraction={self.env_config.sample_fraction}, seed={self.env_config.sample_seed}")
        
        return df

    def read_target_data(self, index_name: str, **kwargs) -> DataFrame:
        physical_path = self._resolve_source_path(index_name)
        logger.debug(f"Mapeando índice ES '{index_name}' para caminho: {physical_path}")        
        
        return self.spark.read.format(self.env_config.source_data_format).load(physical_path)
    
    def read_specific_partition(self, table_name: str, partition_expr: str, **kwargs) -> DataFrame:        
        return self.read_source_data(table_name).filter(partition_expr)

    def get_partitioned_sample(self, table_name: str, fraction: float, seed: Optional[int] = None, **kwargs) -> DataFrame:
        """Gera uma amostragem estatística controlada da tabela especificada."""
        return self.read_source_data(table_name).sample(withReplacement=False, fraction=fraction, seed=seed)   

    def write_data(self, data: DataFrame, output_folder: str, **kwargs) -> None:        
        physical_path = self._resolve_output_path(output_folder)
        data.write.mode("overwrite").format(self.env_config.output_data_format).save(physical_path)

    def exclude_records(self, primary_dataset: DataFrame, records_to_exclude: DataFrame, join_key: str) -> DataFrame:
        return primary_dataset.join(
            records_to_exclude,
            on=primary_dataset[join_key] == records_to_exclude[join_key],
            how="left_anti"
        )