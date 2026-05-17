# src/utils/logging_config.py

import logging
import sys
import warnings
from urllib3.exceptions import InsecureRequestWarning

def setup_logging(level=logging.INFO):
    """
    Configura o logging raiz para direcionar mensagens para um stream (sys.stdout por padrão).

    Por padrão, as mensagens são escritas para `sys.stdout` com nível `INFO`.
    Remove handlers preexistentes do logger raiz para evitar duplicação
    de mensagens caso esta função seja chamada múltiplas vezes.
    Também configura níveis de log para bibliotecas verbosas e suprime warnings específicos.

    Args:
        level (int): O nível mínimo de logging a ser processado (ex: `logging.INFO`, `logging.DEBUG`).
                     Padrão: `logging.INFO`.
    """
    # Define the format for log messages
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Get the root logger instance
    root_logger = logging.getLogger()

    # Remove existing handlers to prevent duplicate logs if setup_logging is called multiple times
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Set the minimum logging level for the root logger
    root_logger.setLevel(level)

    # Create a handler to output logs to the specified stream (sys.stdout)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    # The handler's level doesn't need explicit setting here,
    # as the root logger's level already filters messages passed to handlers.

    # Add the configured handler to the root logger
    root_logger.addHandler(stream_handler)

    # --- Configure log levels for noisy libraries ---

    # Reduce verbosity for the Elasticsearch client logger
    # Set its level to ERROR to avoid overly detailed connection logs, etc.
    # Only critical errors from the ES client will be shown.
    es_client_logger = logging.getLogger('elasticsearch')
    es_client_logger.setLevel(logging.ERROR) # Was ERROR, keeping it

    # Reduce verbosity for the py4j logger (used by PySpark)
    py4j_logger = logging.getLogger('py4j')
    py4j_logger.setLevel(logging.WARNING)

    # Reduce verbosity for the pyspark logger
    pyspark_logger = logging.getLogger('pyspark')
    pyspark_logger.setLevel(logging.WARNING)

    # --- Suppress specific warnings ---

    # Suppress insecure request warnings from Elasticsearch when verify_certs=False
    # Note: Using verify_certs=False is insecure and should be avoided in production.
    # A warning for this is typically logged by the es.client module if used.
    warnings.filterwarnings(
        "ignore",
        message="Connecting to 'https://.*' using TLS with verify_certs=False is insecure",
        module="elasticsearch" # Target the warning specifically from the elasticsearch module
    )

    # Suppress InsecureRequestWarning from urllib3, which is often triggered
    # by Elasticsearch connections with verify_certs=False.
    warnings.filterwarnings(
        "ignore",
        category=InsecureRequestWarning,
        module="urllib3.connectionpool" # More specific module if possible, or general urllib3
    )

    logging.info(f"Root logger level set to {logging.getLevelName(root_logger.getEffectiveLevel())}")
    logging.debug("Logging setup complete. Elasticsearch, Py4J, and PySpark loggers are set to higher severity levels.")

def setup_worker_logging(level=logging.INFO):
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            stream=sys.stdout
        )