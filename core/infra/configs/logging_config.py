import logging
import sys
import os

def configure_logging() -> None:
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    log_format = "%(asctime)s | %(levelname)-5s | %(name)s | %(message)s"
    date_format = "%H:%M:%S"

    
    env_log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    
    level_mapping = logging.getLevelNamesMapping()
    numeric_level = level_mapping.get(env_log_level, logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        datefmt=date_format,
        handlers=[logging.StreamHandler(sys.stdout)]
    )