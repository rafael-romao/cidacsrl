import logging
import time
from typing import Dict, Any

from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError, ConnectionTimeout as ESTimeoutError
from pyspark.sql import DataFrame


from cidacsrl.cidacsrl.infra.adapters.outbound.elasticsearch.client import get_es_client
from cidacsrl.cidacsrl.infra.adapters.outbound.elasticsearch.mapping_models import ESIndexDefinition

logger = logging.getLogger(__name__)


def create_es_index_and_ingest_data(
    es_config: Dict[str, Any],
    index_definition: ESIndexDefinition,
    dataframe_to_index: DataFrame
):
    """
    Cria um índice no cluster Elasticsearch (se ainda não existir) usando o mapeamento
    e as configurações da `ESIndexDefinition` fornecida, e então ingere os dados
    do DataFrame Spark nesse índice.

    Args:
        es_config (Dict[str, Any]): Dicionário com detalhes da conexão Elasticsearch,
                                     compatível com `get_es_client`.
        index_definition (ESIndexDefinition): Um objeto `ESIndexDefinition` contendo
                                              o nome do índice, settings e definições de coluna.
        dataframe_to_index (DataFrame): O DataFrame Spark contendo os dados a serem indexados.

    Raises:
        ValueError: Se configurações essenciais estiverem ausentes.
        ConnectionError: Se a conexão com o Elasticsearch falhar.
        Exception: Para outros erros durante o processo de criação do índice ou ingestão.
    """
    # Validate essential configurations for the Spark-ES writer from es_config
    es_connection_url = es_config.get("es_connection_url")
    if not es_connection_url:
         raise ValueError("Elasticsearch connection URL ('es_connection_url') not found in ES configuration, "
                          "which is required for the Spark-ES connector.")

    logger.info(f"Starting Elasticsearch indexing operation for index: {index_definition.name}")
    logger.info(f"Using Elasticsearch nodes for Spark-ES connector: {es_connection_url}")

    try:
        # Get the body for index creation from the ESIndexDefinition object
        index_creation_body = index_definition.build_index_creation_body()

        # Configuration for the Elasticsearch Spark connector (elasticsearch-hadoop)
        es_hadoop_config: Dict[str, Any] = {
            "es.nodes": es_connection_url,
            "es.resource": index_definition.name,
            "es.nodes.wan.only": "true", # Often useful for cloud or remote clusters
            # "es.mapping.id": "id_column_name" # Set if DataFrame has a dedicated ID column for ES _id
        }
        # Add authentication to Spark-ES connector if present in es_config
        if es_config.get("es_user") and es_config.get("es_password"):
            es_hadoop_config["es.net.http.auth.user"] = es_config.get("es_user")
            es_hadoop_config["es.net.http.auth.pass"] = es_config.get("es_password")

        # Use the centralized get_es_client for ES API operations (check/create index)
        es_api_client: Elasticsearch = get_es_client(es_config)
        if not es_api_client:
            error_msg = (f"Failed to obtain Elasticsearch API client. "
                         f"Cannot check or create index '{index_definition.name}'.")
            logger.error(error_msg)
            raise ConnectionError(error_msg) # Or a more specific custom exception

        # Check if the index exists, create it if not
        if not es_api_client.indices.exists(index=index_definition.name):
            logger.info(f"Index '{index_definition.name}' does not exist. Creating with mapping and settings...")
            logger.debug(f"Index creation body for '{index_definition.name}': {index_creation_body}")
            try:
                es_api_client.indices.create(index=index_definition.name, body=index_creation_body)
                logger.info(f"Index '{index_definition.name}' created successfully.")
            except Exception as e_create:
                logger.error(f"Failed to create index '{index_definition.name}': {e_create}", exc_info=True)
                raise # Re-throw to indicate failure
        else:
            logger.info(f"Index '{index_definition.name}' already exists. "
                        "Data will be overwritten as per Spark save mode ('overwrite').")
            # Note: Updating mappings on an existing index is complex.
            # This workflow assumes either a new index or overwrite compatibility.

        # Save the DataFrame to Elasticsearch using the Spark-ES connector
        logger.info(f"Starting data ingestion into Elasticsearch index '{index_definition.name}'...")
        ingestion_start_time = time.time()

        dataframe_to_index.write.format("org.elasticsearch.spark.sql") \
            .options(**es_hadoop_config) \
            .mode("overwrite") \
            .save() # By default, saves to the index specified in "es.resource"

        ingestion_duration_seconds = time.time() - ingestion_start_time
        ingestion_duration_minutes, remainder_seconds = divmod(int(ingestion_duration_seconds), 60)
        logger.info(
            f"Data ingested into Elasticsearch index '{index_definition.name}' in "
            f"{ingestion_duration_minutes}m {remainder_seconds}s."
        )

    except (ESConnectionError, ESTimeoutError) as e_conn: # Specific ES connection/timeout errors
        logger.error(f"Elasticsearch connection or timeout error during indexing operation "
                     f"for '{index_definition.name}': {e_conn}", exc_info=True)
        raise
    except ValueError as ve: # Catch ValueErrors from config or model validation
        logger.error(f"Configuration error during indexing for '{index_definition.name}': {ve}", exc_info=True)
        raise
    except Exception as e: # Catch other general errors
        logger.error(f"An unexpected error occurred during indexing data into Elasticsearch "
                     f"index '{index_definition.name}': {e}", exc_info=True)
        raise