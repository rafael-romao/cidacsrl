import logging
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


def read_source_data(
    spark: SparkSession,
    source_path: Union[str, Path],
    sample_fraction: Optional[float] = None,
    sample_seed: Optional[int] = None
) -> DataFrame:
    io_manager = DataIOManager(spark)
    
    # Valida se existe
    if not io_manager.exists(source_path):
        raise FileNotFoundError(f"Caminho não encontrado: {source_path}")
    
    # Carrega os dados
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