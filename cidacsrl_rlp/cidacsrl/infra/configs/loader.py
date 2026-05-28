import logging
import yaml
from typing import Dict, Any, Union, List
from pathlib import Path


from cidacsrl_rlp.cidacsrl.infra.configs.models.linkage_workflow_config import LinkageWorkflowConfig
from cidacsrl_rlp.cidacsrl.infra.configs.models.indexed_dataset_filter import parse_indexed_dataset_filter
from cidacsrl_rlp.cidacsrl.domain.models.workflow import SequentialBlockingWorkflow
from cidacsrl_rlp.cidacsrl.infra.elasticsearch.models.service_config import ElasticsearchConfig

logger = logging.getLogger(__name__)

def _load_yaml(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Carrega e valida um arquivo de configuração YAML base.

    Raises:
        FileNotFoundError: Se o arquivo não for encontrado.
        ValueError: Se o caminho não for arquivo, não for YAML, estiver vazio
                    ou o conteúdo não for um dicionário.
        IOError: Se ocorrer erro de leitura.
    """
    path_obj = Path(file_path).resolve()
    file_name = path_obj.name

    if not path_obj.exists():
        raise FileNotFoundError(
            f"Configuration file '{file_name}' not found at {path_obj.parent}."
        )
    if not path_obj.is_file():
        raise ValueError(f"Path '{path_obj}' is not a valid file.")
    if path_obj.suffix.lower() not in (".yaml", ".yml"):
        raise ValueError(
            f"File '{file_name}' must be a YAML file (.yaml or .yml). "
            f"Found suffix: '{path_obj.suffix}'"
        )

    try:
        with open(path_obj, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file '{file_name}': {e}") from e
    except IOError as e:
        raise IOError(f"Error reading file '{file_name}': {e}") from e
    except Exception as e:
        raise IOError(f"Unexpected error reading file '{file_name}': {e}") from e

    if config_data is None:
        raise ValueError(f"Configuration file '{file_name}' is empty.")
    if not isinstance(config_data, dict):
        raise ValueError(
            f"Invalid configuration format in '{file_name}'. "
            f"Expected a dictionary, got {type(config_data).__name__}."
        )

    logger.debug(f"YAML loaded successfully from '{path_obj}'.")
    return config_data

def _validate_required_keys(
    config_data: Dict[str, Any], required_keys: List[str], file_name: str
) -> None:
    missing = [k for k in required_keys if k not in config_data]
    if missing:
        raise ValueError(f"Missing required keys in '{file_name}': {missing}")


def parse_linkage_workflow_config(data: Dict[str, Any]) -> LinkageWorkflowConfig:
    _validate_required_keys(
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
    _validate_required_keys(
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


def parse_es_config(data: Dict[str, Any]) -> ElasticsearchConfig:
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
    return ElasticsearchConfig(**data)


def load_linkage_workflow_config(path: Union[str, Path]) -> LinkageWorkflowConfig:
    return parse_linkage_workflow_config(_load_yaml(path))


def load_sequential_blocking_workflow_config(path: Union[str, Path]) -> SequentialBlockingWorkflow:
    return parse_sequential_blocking_workflow_config(_load_yaml(path))


def load_es_config(path: Union[str, Path]) -> ElasticsearchConfig:
    return parse_es_config(_load_yaml(path))