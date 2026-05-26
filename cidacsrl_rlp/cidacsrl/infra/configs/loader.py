import logging
from pathlib import Path
from typing import Dict, Any, Union

from cidacsrl_rlp.shared.infra.config_loader import load_yaml, validate_required_keys
from cidacsrl_rlp.cidacsrl.infra.configs.models.linkage_workflow_config import LinkageWorkflowConfig
from cidacsrl_rlp.cidacsrl.infra.configs.models.indexed_dataset_filter import parse_indexed_dataset_filter
from cidacsrl_rlp.cidacsrl.domain.models.workflow import SequentialBlockingWorkflow
from cidacsrl_rlp.cidacsrl.infra.elasticsearch.models.service_config import ElasticsearchServiceConfig

logger = logging.getLogger(__name__)


def parse_linkage_workflow_config(data: Dict[str, Any]) -> LinkageWorkflowConfig:
    """
    Valida e deserializa um dicionário em um LinkageWorkflowConfig tipado.

    Raises:
        ValueError: Se chaves obrigatórias estiverem ausentes ou a estrutura for inválida.
    """
    validate_required_keys(
        data,
        required_keys=[
            "linkage_config_path",
            "es_config_path",
            "spark_config_path",
            "source_data_path",
            "output_data_path",
            "source_data_format",
        ],
        file_name="LinkageWorkflowConfig",
    )

    logger.debug("Parsing LinkageWorkflowConfig.")

    try:
        return LinkageWorkflowConfig(**data)
    except TypeError as e:
        raise ValueError(f"Invalid structure for LinkageWorkflowConfig: {e}") from e


def parse_sequential_blocking_workflow_config(data: Dict[str, Any]) -> SequentialBlockingWorkflow:
    """
    Valida e deserializa um dicionário em um SequentialBlockingWorkflow tipado.

    Raises:
        ValueError: Se chaves obrigatórias estiverem ausentes ou a estrutura for inválida.
    """
    validate_required_keys(
        data,
        required_keys=[
            "source_table",
            "id_source_table",
            "target_es_index",
            "id_target_table",
            "blocking_phases",
        ],
        file_name="SequentialBlockingWorkflow",
    )

    logger.debug("Parsing SequentialBlockingWorkflow.")

    try:
        parsed_data = data.copy()
        parsed_data["indexed_dataset_filter"] = parse_indexed_dataset_filter(
            parsed_data.get("indexed_dataset_filter")
        )
        return SequentialBlockingWorkflow.from_dict(parsed_data)
    except (TypeError, ValueError) as e:
        raise ValueError(
            f"Invalid structure for SequentialBlockingWorkflow: {e}"
        ) from e

def parse_es_config(data: Dict[str, Any]) -> ElasticsearchServiceConfig:
    """
    Valida e deserializa um dicionário em um ElasticsearchServiceConfig tipado.

    Raises:
        ValueError: Se chaves obrigatórias estiverem ausentes ou a estrutura for inválida.
    """
    validate_required_keys(data, required_keys=["es_connection_url"], file_name="ElasticsearchServiceConfig")

    logger.debug("Parsing ElasticsearchServiceConfig.")

    try:
        return ElasticsearchServiceConfig(**data)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid structure for ElasticsearchServiceConfig: {e}") from e


def load_linkage_workflow_config(path: Union[str, Path]) -> LinkageWorkflowConfig:
    return parse_linkage_workflow_config(load_yaml(path))


def load_sequential_blocking_workflow_config(path: Union[str, Path]) -> SequentialBlockingWorkflow:
    return parse_sequential_blocking_workflow_config(load_yaml(path))


def load_es_config(path: Union[str, Path]) -> ElasticsearchServiceConfig:
    return parse_es_config(load_yaml(path))
