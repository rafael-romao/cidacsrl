import yaml
import logging
from typing import Union, Dict, Any, List
from pathlib import Path

logger = logging.getLogger(__name__)


def validate_required_keys(
    config_data: Dict[str, Any], required_keys: List[str], file_name: str
) -> None:
    missing = [k for k in required_keys if k not in config_data]
    if missing:
        raise ValueError(f"Missing required keys in '{file_name}': {missing}")


# ─── Base ─────────────────────────────────────────────────────────────────────

def load_yaml(file_path: Union[str, Path]) -> Dict[str, Any]:
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


# ─── Domain configs ───────────────────────────────────────────────────────────

def load_column_config(
    config_file: Union[str, Path],
) -> List[Union[ColumnConfig, ConcatenateColumnConfig]]:
    config = load_yaml(config_file)
    file_name = Path(config_file).name

    columns2clean = config.get("columns")
    if not isinstance(columns2clean, list):
        raise ValueError(
            f"Missing or invalid 'columns' list in '{file_name}'."
        )
    if not columns2clean:
        raise ValueError(f"No columns defined under 'columns' key in '{file_name}'.")

    columns_config = []
    try:
        for col in columns2clean:
            columns_config.append(ColumnConfig(**col))
    except TypeError as e:
        raise ValueError(
            f"Invalid data structure for an item in 'columns' in '{file_name}': {e}"
        ) from e

    columns2concatenate = config.get("concatenate")
    if columns2concatenate:
        if not isinstance(columns2concatenate, list):
            raise ValueError(
                f"Invalid 'concatenate' in '{file_name}'. Expected a list."
            )
        try:
            for conc in columns2concatenate:
                columns_config.append(ConcatenateColumnConfig(**conc))
        except TypeError as e:
            raise ValueError(
                f"Invalid data structure for 'concatenate' in '{file_name}': {e}"
            ) from e

    return columns_config


def load_index_config(config_file_path: Union[str, Path]) -> ESIndexDefinition:
    config_dict = load_yaml(config_file_path)
    file_name = Path(config_file_path).name
    logger.info(f"Loading Elasticsearch index definition from: {file_name}")

    validate_required_keys(config_dict, ["columns", "index_config"], file_name)

    columns_data = config_dict["columns"]
    if not isinstance(columns_data, list) or not columns_data:
        raise ValueError(
            f"'columns' must be a non-empty list in '{file_name}'."
        )

    index_config_data = config_dict["index_config"]
    if not isinstance(index_config_data, dict):
        raise ValueError(
            f"'index_config' must be a dictionary in '{file_name}'."
        )

    index_name = index_config_data.get("name")  # .get() — não muta o dict
    if not index_name:
        raise ValueError(f"'name' is missing in 'index_config' section of '{file_name}'.")

    try:
        column_definitions = [ESColumnDefinition(**col) for col in columns_data]
        settings_data = {k: v for k, v in index_config_data.items() if k != "name"}
        index_definition = ESIndexDefinition(
            name=index_name,
            settings=ESIndexSettings(**settings_data),
            columns=column_definitions,
        )
    except TypeError as e:
        raise ValueError(
            f"Invalid structure in '{file_name}'. "
            f"Check keys in 'columns' or 'index_config': {e}"
        ) from e

    logger.info(f"Index definition '{index_definition.name}' loaded from '{file_name}'.")
    return index_definition

def load_elasticsearch_indexing_workflow_config(
    config_path: Union[str, Path],
) -> ElasticsearchIndexingWorkflowConfig:
    config_data = load_yaml(config_path)
    file_name = Path(config_path).name

    validate_required_keys(
        config_data,
        ["es_config_path", "spark_config_path", "index_config_path", "source_data_path"],
        file_name,
    )

    try:
        return ElasticsearchIndexingWorkflowConfig(**config_data)
    except TypeError as e:
        raise ValueError(f"Invalid config in '{file_name}': {e}") from e


def load_deduplicate_workflow_config(
    config_path: Union[str, Path],
) -> DeduplicateWorkflowConfig:
    config_data = load_yaml(config_path)
    file_name = Path(config_path).name

    validate_required_keys(
        config_data,
        ["spark_config_path", "source_data_path", "output_data_path"],
        file_name,
    )

    try:
        return DeduplicateWorkflowConfig(**config_data)
    except TypeError as e:
        raise ValueError(f"Invalid config in '{file_name}': {e}") from e