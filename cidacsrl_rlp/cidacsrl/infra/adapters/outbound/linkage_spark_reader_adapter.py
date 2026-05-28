from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_reader_port import DataReaderPort
from cidacsrl_rlp.shared.infra.spark.spark_io_adapter import SparkIOAdapter
from cidacsrl_rlp.cidacsrl.infra.configs.models.linkage_workflow_config import LinkageWorkflowConfig
from typing import Any
import logging

logger = logging.getLogger(__name__)

class LinkageSparkReaderAdapter(DataReaderPort):
    def __init__(self, spark_io: SparkIOAdapter, workflow_config: LinkageWorkflowConfig):
        self.spark_io = spark_io
        self.source_data_path = workflow_config.source_data_path
        self.source_data_format = workflow_config.source_data_format

    def read_data(self) -> Any:

        format_factory = {
            "csv": self.spark_io.read_csv,
            "parquet": self.spark_io.read_parquet
        }

        if self.source_data_format in format_factory:
            logger.debug(f"Reading {self.source_data_format.upper()} data from source: {self.source_data_path}")
            return format_factory[self.source_data_format](self.source_data_path)
        else:
            logger.error(f"Unsupported data format: {self.source_data_format}")
            raise ValueError(f"Unsupported data format: {self.source_data_format}")