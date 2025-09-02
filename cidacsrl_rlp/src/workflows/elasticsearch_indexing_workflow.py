import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession

from cidacsrl_rlp.src.config.loader import load_index_config, load_service_config
from cidacsrl_rlp.src.es.indexing_operations import create_es_index_and_ingest_data
from cidacsrl_rlp.src.utils.logging_config import setup_logging
from cidacsrl_rlp.src.utils.spark_utils import create_spark_session

# Configure logging
logger = logging.getLogger(__name__)


def main():
    """Ponto de entrada principal para o fluxo de trabalho de indexação no Elasticsearch.

    Este script orquestra o processo de indexação de dados de um diretório
    Parquet para um índice no Elasticsearch. Ele é projetado para ser executado
    a partir da linha de comando e realiza as seguintes operações:

    1.  Carrega as configurações do Spark, do Elasticsearch e a definição do
        índice a partir de arquivos YAML.
    2.  Inicializa uma sessão Spark com as configurações fornecidas.
    3.  Lê os dados da fonte (de um diretório Parquet) para um DataFrame Spark.
    4.  Cria um novo índice no Elasticsearch (se não existir) com os mapeamentos
        e configurações definidos.
    5.  Ingesta os dados do DataFrame no índice do Elasticsearch.

    Args:
        --source-data-path (str): Caminho para o diretório Parquet de origem.
        --index-config-path (str): Caminho para o arquivo de configuração YAML
            do índice do Elasticsearch.
        --spark-config-path (str): Caminho para o arquivo de configuração YAML do Spark.
        --es-config-path (str): Caminho para o arquivo de configuração YAML da
            conexão com o Elasticsearch.
        --log-level (str): Nível de logging para a aplicação (padrão: "INFO").

    Example:
        Para executar o fluxo de trabalho a partir do terminal:

        .. code-block:: bash

            python -m cidacsrl_rlp.src.workflows.elasticsearch_indexing_workflow \\
                --source-data-path /path/to/trusted_data.parquet \\
                --index-config-path /path/to/your_index.yaml \\
                --spark-config-path /path/to/your_spark_config.yaml \\
                --es-config-path /path/to/your_es_config.yaml \\
                --log-level DEBUG
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Indexes datasets from a Parquet source into Elasticsearch."
    )
    parser.add_argument(
        "--source-data-path",
        required=True,
        help="The path to the source Parquet directory (trusted zone data).",
    )
    parser.add_argument(
        "--index-config-path",
        required=True,
        help="The path to the Elasticsearch index configuration YAML file.",
    )
    parser.add_argument(
        "--spark-config-path",
        required=True,
        help="The path to the Spark configuration YAML file.",
    )
    parser.add_argument(
        "--es-config-path",
        required=True,
        help="The path to the Elasticsearch connection configuration YAML file.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level for the application (default: INFO).",
    )
    args = parser.parse_args()

    setup_logging(level=getattr(logging, args.log_level.upper()))
    logger.info(f"Starting indexing workflow with arguments: {args}")

    # Validate if the source data path exists
    source_data_path = Path(args.source_data_path)
    if not source_data_path.exists():
        logger.error(f"Source data path '{source_data_path}' does not exist.")
        raise FileNotFoundError(f"Source data path '{source_data_path}' does not exist.")
    if not source_data_path.is_dir():
        logger.error(f"Source data path '{source_data_path}' is not a valid directory.")
        raise ValueError(
            f"Source data path '{source_data_path}' is not a valid directory."
        )

    # Load configurations
    logger.info("Loading configurations...")
    index_definition = load_index_config(args.index_config_path)
    es_connection_config = load_service_config(
        args.es_config_path, service_name="elasticsearch"
    )

    # Initialize Spark session
    spark = create_spark_session(
        app_name=f"IndexerApp-{index_definition.name}",
        spark_config_path=args.spark_config_path,
    )

    # Load source data from Parquet
    try:
        logger.info(f"Reading source data from Parquet directory: {source_data_path}")
        df_source = spark.read.format("parquet").load(str(source_data_path))
        logger.info(f"Source data loaded successfully with {df_source.count():,} records.")
    except Exception as e:
        logger.error(
            f"Failed to load source data from '{source_data_path}': {e}", exc_info=True
        )
        spark.stop()
        exit(1)  # Exit with error

    # Check if the DataFrame is empty or has no columns
    if not df_source.columns:
        logger.error("Source DataFrame is empty or has no columns.")
        spark.stop()
        raise ValueError("Source DataFrame for indexing cannot be empty or without columns.")

    # Call the function to create index and ingest data
    try:
        logger.info(
            f"Starting Elasticsearch index creation and data ingestion for index '{index_definition.name}'..."
        )
        create_es_index_and_ingest_data(
            es_config=es_connection_config,
            index_definition=index_definition,
            dataframe_to_index=df_source,
        )
        logger.info(
            f"Successfully completed indexing for index '{index_definition.name}'."
        )
    except Exception as e:
        logger.critical(f"Indexing workflow failed: {e}", exc_info=True)
        spark.stop()
        exit(1)
    finally:
        if SparkSession.getActiveSession():
            logger.info("Stopping SparkSession...")
            spark.stop()
            logger.info("SparkSession stopped.")


if __name__ == "__main__":
    main()