import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession

from cidacsrl_rlp.shared.infra.config_loader import load_index_config, load_service_config, load_elasticsearch_indexing_workflow_config
from cidacsrl_rlp.cidacsrl.infra.elasticsearch.indexing_operations import create_es_index_and_ingest_data
from cidacsrl_rlp.src.utils.logging_config import setup_logging
from cidacsrl_rlp.cidacsrl.infra.spark.utils import create_spark_session

# Configure logging
logger = logging.getLogger(__name__)


def main():
    """Ponto de entrada principal para o fluxo de trabalho de indexação no Elasticsearch.

    Este script orquestra o processo de indexação de dados de um diretório
    Parquet para um índice no Elasticsearch. Ele é projetado para ser executado
    a partir da linha de comando e realiza as seguintes operações:

    1.  Carrega as configurações do workflow a partir de um arquivo YAML central.
    2.  Carrega as configurações do Spark, do Elasticsearch e a definição do
        índice a partir dos arquivos especificados na configuração.
    3.  Inicializa uma sessão Spark com as configurações fornecidas.
    4.  Lê os dados da fonte (de um diretório Parquet) para um DataFrame Spark.
    5.  Cria um novo índice no Elasticsearch (se não existir) com os mapeamentos
        e configurações definidos.
    6.  Ingesta os dados do DataFrame no índice do Elasticsearch.

    Args:
        --config-path (str): Caminho para o arquivo de configuração YAML principal
            do workflow que contém os caminhos para todos os outros arquivos de
            configuração.
        --log-level (str): Nível de logging para a aplicação (padrão: "INFO").

    Example:
        Para executar o fluxo de trabalho a partir do terminal:

        .. code-block:: bash

            python -m cidacsrl_rlp.src.workflows.elasticsearch_indexing_workflow \\
                --config-path /path/to/workflow_config.yaml \\
                --log-level DEBUG

        O arquivo de configuração do workflow deve conter:

        .. code-block:: yaml

            es_config_path: "/path/to/elasticsearch_connection_config.yaml"
            spark_config_path: "/path/to/spark_config.yaml"
            index_config_path: "/path/to/elasticsearch_index_config.yaml"
            source_data_path: "/path/to/trusted_data.parquet"
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Indexes datasets from a Parquet source into Elasticsearch."
    )
    parser.add_argument(
        "--config-path",
        required=True,
        help="The path to the workflow configuration YAML file containing all necessary paths and settings.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level for the application (default: INFO).",
    )
    args = parser.parse_args()

    setup_logging(level=getattr(logging, args.log_level.upper()))
    logger.info(f"Starting indexing workflow with config: {args.config_path}")

    # Load workflow configuration
    try:
        logger.info("Loading workflow configuration...")
        workflow_config = load_elasticsearch_indexing_workflow_config(args.config_path)
        logger.info(f"Workflow configuration loaded successfully for ES config: {workflow_config.es_config_path}")
    except (FileNotFoundError, ValueError, IOError) as e:
        logger.error(f"Failed to load workflow configuration: {e}")
        exit(1)

    # Validate if the source data path exists
    source_data_path = Path(workflow_config.source_data_path)
    if not source_data_path.exists():
        logger.error(f"Source data path '{source_data_path}' does not exist.")
        raise FileNotFoundError(f"Source data path '{source_data_path}' does not exist.")
    if not source_data_path.is_dir():
        logger.error(f"Source data path '{source_data_path}' is not a valid directory.")
        raise ValueError(
            f"Source data path '{source_data_path}' is not a valid directory."
        )

    # Load configurations
    logger.info("Loading individual configurations...")
    index_definition = load_index_config(workflow_config.index_config_path)
    es_connection_config = load_service_config(
        workflow_config.es_config_path, service_name="elasticsearch"
    )

    # Initialize Spark session
    spark = create_spark_session(
        app_name=f"IndexerApp-{index_definition.name}",
        spark_config_path=workflow_config.spark_config_path,
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