import argparse
import logging
import time

from pyspark.sql import SparkSession

from cidacsrl_rlp.src.config.loader import load_service_config
from cidacsrl_rlp.src.utils.logging_config import setup_logging


