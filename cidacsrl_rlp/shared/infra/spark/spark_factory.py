from pyspark.sql import SparkSession
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

def log_spark_configs(spark: SparkSession):
    """
    Registra as configurações da SparkSession ativa.

    Args:
        spark (SparkSession): A sessão Spark ativa.
    """
    logger.debug("Configurações do Spark ativas:")
    configs = spark.sparkContext.getConf().getAll()
    for key, value in sorted(configs):
        if key.startswith("spark."):
            logger.debug(f"{key}: {value}")


def create_spark_session(
    app_name: str,
    spark_config: Optional[Dict[str, Any]] = None,
    checkpoint_dir: Optional[str] = None,
) -> SparkSession:
    """
    Cria e configura uma SparkSession a partir de um arquivo de configuração.
    Agnóstico à estrutura: suporta dicionários planos ou com a chave 'spark_configs'.

    Args:
        app_name (str): O nome da aplicação Spark.
        spark_config (Optional[Dict[str, Any]]): Configurações do Spark.
        checkpoint_dir (Optional[str]): Caminho para o diretório de checkpoint do Spark.

    Returns:
        SparkSession: A instância da SparkSession configurada.
    """
    spark_builder = SparkSession.builder.appName(app_name)

    if spark_config:
        loaded_spark_configs = spark_config.get("spark_configs", spark_config)
        
        if isinstance(loaded_spark_configs, dict):
            for key, value in loaded_spark_configs.items():                
                if isinstance(key, str) and key.startswith("spark."):
                    spark_builder = spark_builder.config(key, str(value))
        else:
            logger.warning(
                "Nenhuma configuração válida encontrada ou não é um dicionário. "
                "Usando padrões do Spark."
            )

    spark = spark_builder.getOrCreate()
    logger.info(f"SparkSession '{app_name}' criada com sucesso.")
    
    master_config = spark.conf.get("spark.master", "local[*]")
    logger.info(f"Spark executando no modo: {master_config}")

    if checkpoint_dir:
        try:
            spark.sparkContext.setCheckpointDir(checkpoint_dir)
            logger.info(f"Diretório de checkpoint do Spark definido para: {checkpoint_dir}")
        except Exception as e:
            logger.warning(
                f"Não foi possível definir o diretório de checkpoint '{checkpoint_dir}': {e}"
            )

    log_spark_configs(spark)

    return spark