from cidacsrl_rlp.cidacsrl.infra.configs.loader import (
    load_linkage_workflow_config,
    load_sequential_blocking_workflow_config,
    load_es_config,
)
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.client import get_es_client
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.linkage_spark_reader_adapter import LinkageSparkReaderAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_search_adapter import SparkESSearchAdapter
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_scoring_adapter import SparkScoringAdapter
from cidacsrl_rlp.cidacsrl.application.use_cases.run_sequential_linkage_workflow import RunSequentialLinkageWorkflowUseCase
from cidacsrl_rlp.shared.infra.spark.spark_io_adapter import SparkIOAdapter
from dataclasses import asdict
from typing import Any
import logging



logger = logging.getLogger(__name__)


def run_likage(config_path: str, spark: Any):
    workflow_config  = load_linkage_workflow_config(config_path)
    blocking_config  = load_sequential_blocking_workflow_config(workflow_config.linkage_config_path)
    es_config        = load_es_config(workflow_config.es_config_path)

    test_client = get_es_client(es_config, use_cache=False)
    if not test_client:
        raise ConnectionError("Falha crítica de conectividade com o Elasticsearch antes da inicialização do pipeline.")


    spark_io = SparkIOAdapter(spark)
    reader_adapter = LinkageSparkReaderAdapter(spark_io, workflow_config)
    search_adapter = SparkESSearchAdapter(
        index_name=blocking_config.target_es_index,
        es_config=asdict(es_config),
    )
    scoring_adapter = SparkScoringAdapter() 

    linkage_execution = RunSequentialLinkageWorkflowUseCase(reader_adapter, search_adapter, scoring_adapter)
    linkage_execution.execute(blocking_config)    
