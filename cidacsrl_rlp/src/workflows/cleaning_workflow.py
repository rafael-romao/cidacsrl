import argparse
import logging
import time

from pyspark.sql import SparkSession

from cidacsrl_rlp.src.cleaning.column_cleaner import ColumnCleanerPipeline
from cidacsrl_rlp.src.config.loader import (load_column_config,
                                            load_service_config)
from cidacsrl_rlp.src.utils.logging_config import setup_logging

# Configure logging
logger = logging.getLogger(__name__)


def main():
    """Ponto de entrada principal para o fluxo de limpeza de dados.

    Este script executa um pipeline de limpeza de dados em um arquivo Parquet
    usando o Apache Spark. Ele carrega as configurações do Spark e das colunas,
    inicializa uma sessão Spark, lê os dados brutos, aplica as transformações
    de limpeza definidas e salva o resultado em um novo arquivo Parquet.

    O script é projetado para ser executado a partir da linha de comando,
    recebendo os caminhos para os arquivos de configuração e dados como
    argumentos.

    Args:
        --spark-config-path (str): Caminho para o arquivo de configuração do Spark.
        --columns-config-path (str): Caminho para o arquivo de configuração das colunas a serem limpas.
        --source-data-path (str): Caminho para a tabela de dados brutos em formato Parquet.
        --output-data-path (str): Caminho para salvar a tabela de dados limpos em formato Parquet.
        --log-level (str): Nível de logging para a aplicação. Opções: "DEBUG",
            "INFO", "WARNING", "ERROR", "CRITICAL". O padrão é "INFO".

    Example:
        Para executar o fluxo de limpeza a partir do terminal:

        .. code-block:: bash

            python -m cidacsrl_rlp.src.workflows.cleaning_workflow \\
                --spark-config-path /path/to/spark_config.yaml \\
                --columns-config-path /path/to/columns_config.yaml \\
                --source-data-path /path/to/raw_data.parquet \\
                --output-data-path /path/to/cleaned_data.parquet \\
                --log-level DEBUG

    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Cleaning datasets.")
    parser.add_argument(
        "--spark-config-path",
        required=True,
        help="The path to the spark configuration file.",
    )
    parser.add_argument(
        "--columns-config-path",
        required=True,
        help="The path to the column configuration file.",
    )
    parser.add_argument(
        "--source-data-path", required=True, help="The path to the raw table."
    )
    parser.add_argument(
        "--output-data-path",
        required=True,
        help="The path to save the cleaned table.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level for the application (default: INFO).",
    )
    args = parser.parse_args()
    setup_logging(level=getattr(logging, args.log_level.upper()))

    # Load the configuration file
    logger.info("Loading configurations...")
    columns_config = load_column_config(args.columns_config_path)
    spark_config = load_service_config(args.spark_config_path, service_name="spark")

    # Initialize Spark session with Spark configurations
    logger.info("Initializing SparkSession...")
    spark_builder = SparkSession.builder.appName("Cleaner App")
    for key, value in spark_config.items():
        spark_builder = spark_builder.config(key, value)
    spark = spark_builder.getOrCreate()

    # Log Spark configurations
    logger.info("Spark configurations:")
    for key, value in spark_config.items():
        logger.info(f"{key}: {value}")

    # Create the column cleaner pipeline
    cleaner_pipeline = ColumnCleanerPipeline(columns_config)
    raw_columns = [col.name for col in cleaner_pipeline.columns]
    cleaned_columns = ["id_table"] + [
        col.cleaned_name for col in cleaner_pipeline.columns
    ]

    # Load data
    try:
        df_raw = spark.read.format("parquet").load(args.source_data_path)
    except Exception as e:
        logger.error(f"Failed to load raw data: {e}")
        exit(1)
    logger.info(f"Raw data loaded with {df_raw.count():,} records.")

    # Check if the raw columns exist in the DataFrame
    if not all(col in df_raw.columns for col in raw_columns):
        logger.warning(
            f"Columns {raw_columns} not found in raw data. "
            "Selecting from the struct column 'endereco' instead."
        )
        try:
            df_raw = df_raw.select("endereco.*").select(*raw_columns)
        except Exception as e:
            logger.error(f"Failed to select columns: {e}")
            exit(1)
    else:
        df_raw = df_raw.select(*raw_columns)
    logger.info(f"Selected columns: {raw_columns}")

    # Apply the cleaning pipeline
    try:
        df_cleaned = cleaner_pipeline.apply(df_raw).select(*cleaned_columns)
        logger.info(
            f"Pipeline applied. Final columns selected. Dataframe with {df_cleaned.count():,} records."
        )
    except Exception as e:
        logger.error(f"Failed to apply cleaning pipeline: {e}")
        exit(1)

    logger.info("Writing data...")
    write_start_time = time.time()
    df_cleaned.write.mode("overwrite").format("parquet").save(
        args.output_data_path, compression="snappy"
    )
    write_duration_seconds = time.time() - write_start_time
    write_minutes, write_seconds = divmod(write_duration_seconds, 60)
    logger.info(
        f"Data written to {args.output_data_path} in {write_minutes} minutes and {write_seconds:.2f} seconds."
    )


if __name__ == "__main__":
    main()
