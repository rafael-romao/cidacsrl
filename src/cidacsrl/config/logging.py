import logging
import os
import sys


def configure_logging() -> None:
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    log_format = "%(asctime)s | %(levelname)-5s | %(name)-32s | %(message)s"
    date_format ='%Y-%m-%d %H:%M:%S'

    
    env_log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    
    level_mapping = logging.getLevelNamesMapping()
    numeric_level = level_mapping.get(env_log_level, logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        datefmt=date_format,
        handlers=[logging.StreamHandler(sys.stdout)]
    )