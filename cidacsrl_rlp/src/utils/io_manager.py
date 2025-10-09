import logging
import re
import os
from pathlib import Path
from typing import Optional, Union
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)

class DataIOManager:    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read(self, path: Union[str, Path], **kwargs) -> DataFrame:
        path_str = str(path)
        
        try:
            df = self.spark.read.parquet(path_str, **kwargs)            
            logger.info(f"Dados carregados de {path_str}")
            return df            
        except Exception as e:
            logger.error(f"Erro ao ler {path_str}: {e}")
            raise
    
    def write(
        self, 
        df: DataFrame, 
        path: Union[str, Path], 
        mode: str = "overwrite",
        **kwargs
    ) -> None:
        path_str = str(path)
        
        try:
            
            df.write.mode(mode).parquet(path_str, **kwargs)            
            logger.info(f"Dados salvos em {path_str}")
            
        except Exception as e:
            logger.error(f"Erro ao escrever {path_str}: {e}")
            raise
    
    def exists(self, path: Union[str, Path]) -> bool:
        """Verifica se o caminho existe."""
        return Path(path).exists()

def ler_particao(path, partition_by, partition, io_manager):
    import pyspark.sql.functions as F

    if partition == "*":
        # Serão consideradas todas as partições (sem filtro)
        read_path = str(path)
        logger.info(f"> Lendo dados em: {path}")
        return io_manager.read(read_path)
    else:
        read_path = re.sub(r"\/$", "", str(path)) + "/{partition_by}={partition}"
        # Verifica se os dados de origem foram particionados na escrita
        if os.path.exists(read_path):
            logger.info(f"> Lendo dados em: {read_path}")
            return io_manager.read(read_path).withColumn(partition_by, F.lit(partition))
        else:
            # Os dados fonte não foram particionados na escrita então serão lidos e filtrados de acordo com o valor da partição
            read_path = "/".join(read_path.split("/")[:-1])
            logger.info(f"Lendo dados em {read_path} e filtrando partição: {partition_by}={partition}")
            return io_manager.read(read_path).filter(f"{partition_by}={partition}")

def read_source_data(
    spark: SparkSession,
    source_path: Union[str, Path],
    sample_fraction: Optional[float] = None,
    sample_seed: Optional[int] = None,
    partition_info: Optional[dict] = None,
) -> DataFrame:
    io_manager = DataIOManager(spark)
    
    # Valida se existe
    if not io_manager.exists(source_path):
        raise FileNotFoundError(f"Caminho não encontrado: {source_path}")
    
    # Carrega os dados
    if partition_info:
        df = ler_particao(source_path, partition_info['partition_by'], partition_info['partition'], io_manager)
    else:
        df = io_manager.read(source_path)
    
    # Aplica amostragem se configurada
    if sample_fraction and 0.0 < sample_fraction <= 1.0:
        df = df.sample(
            withReplacement=False, 
            fraction=sample_fraction, 
            seed=sample_seed
        )
        logger.info(f"Amostragem aplicada: {sample_fraction}")
    
    return df

def write_phase_results(
    spark: SparkSession,
    df: DataFrame,
    output_path: Union[str, Path],
    mode: str = "overwrite",
    **kwargs
) -> None:
    """
    Função utilitária para escrever resultados de fases de linkage.
    
    Args:
        spark: Sessão Spark
        df: DataFrame a ser escrito
        output_path: Caminho de destino
        mode: Modo de escrita ("overwrite", "append", etc.)
        **kwargs: Argumentos adicionais para escrita
    """
    io_manager = DataIOManager(spark)
    io_manager.write(df, output_path, mode=mode, **kwargs)