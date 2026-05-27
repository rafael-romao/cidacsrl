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
    return LinkageWorkflowConfig(**data)


def parse_sequential_blocking_workflow_config(data: Dict[str, Any]) -> SequentialBlockingWorkflow:
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
    parsed_data = data.copy()
    parsed_data["indexed_dataset_filter"] = parse_indexed_dataset_filter(
        parsed_data.get("indexed_dataset_filter")
    )
    return SequentialBlockingWorkflow.from_dict(parsed_data)


def parse_es_config(data: Dict[str, Any]) -> ElasticsearchServiceConfig:
    # 1. Validação de presença obrigatória básica
    if "es_connection_url" not in data and "cloud_id" not in data:
        raise ValueError("A configuração do Elasticsearch deve conter 'es_connection_url' ou 'cloud_id'.")

    # 2. Validações de Regra de Negócio (Sanity Checks)
    if "es_connection_url" in data:
        url = data["es_connection_url"]
        if not url or not url.startswith(("http://", "https://")):
            raise ValueError(f"'es_connection_url' inválida: '{url}'. Deve começar com 'http://' ou 'https://'.")

    if data.get("request_timeout", 60) <= 0:
        raise ValueError("'request_timeout' deve ser um valor positivo.")

    if data.get("msearch_batch_size", 100) <= 0:
        raise ValueError("'msearch_batch_size' deve ser um valor positivo.")

    # 3. Cast seguro para o TypedDict
    return ElasticsearchServiceConfig(**data)


def load_linkage_workflow_config(path: Union[str, Path]) -> LinkageWorkflowConfig:
    return parse_linkage_workflow_config(load_yaml(path))


def load_sequential_blocking_workflow_config(path: Union[str, Path]) -> SequentialBlockingWorkflow:
    return parse_sequential_blocking_workflow_config(load_yaml(path))


def load_es_config(path: Union[str, Path]) -> ElasticsearchServiceConfig:
    return parse_es_config(load_yaml(path))