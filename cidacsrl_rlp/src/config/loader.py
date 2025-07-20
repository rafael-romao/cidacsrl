import yaml
import logging
from typing import Union, Dict, Any, List
from pathlib import Path
from cidacsrl_rlp.src.cleaning.column_cleaner import ColumnConfig, ConcatenateColumnConfig


from geo_cidacsrl.src.linkage.models import (
    SequentialBlockingWorkflow,
    load_workflow_from_dict
)

from geo_cidacsrl.src.es.mapping_models import (
    ESIndexDefinition,
    ESIndexSettings,
    ESColumnDefinition
)

# Module-level logger
logger = logging.getLogger(__name__)


def load_yaml(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Carrega e valida um arquivo de configuração YAML base.

    Args:
        file_path: O caminho para o arquivo YAML.

    Returns:
        Um dicionário contendo os dados carregados do arquivo YAML.

    Raises:
        FileNotFoundError: Se o arquivo de configuração não for encontrado.
        ValueError: Se o caminho não for um arquivo, não for YAML, estiver vazio ou
                    o conteúdo não for um dicionário.
        IOError: Se ocorrer um erro ao ler o arquivo.
        yaml.YAMLError: Se ocorrer um erro durante o parsing do YAML.
    """
    path_obj = Path(file_path).resolve()
    file_name = path_obj.name

    if not path_obj.exists():
        logger.error(f"Configuration file not found: {path_obj}")
        raise FileNotFoundError(f"Configuration file '{file_name}' not found at {path_obj.parent}.")

    if not path_obj.is_file():
        logger.error(f"Path is not a file: {path_obj}")
        raise ValueError(f"Path '{path_obj}' is not a valid file.")

    if path_obj.suffix.lower() not in (".yaml", ".yml"):
        logger.error(f"File is not YAML: {path_obj}")
        raise ValueError(
            f"File '{file_name}' must be a YAML file (.yaml or .yml). Found suffix: '{path_obj.suffix}'"
        )

    try:
        with open(path_obj, "r", encoding="utf-8") as file:
            config_data = yaml.safe_load(file)
    except yaml.YAMLError as e:
        logger.error(f"YAML parsing error in file '{path_obj}': {e}", exc_info=True)
        # Re-raise to allow specific handling or to propagate a clear error
        raise ValueError(f"Error parsing YAML file '{file_name}': {e}") from e
    except IOError as e:
        logger.error(f"Error reading file '{path_obj}': {e}", exc_info=True)
        raise IOError(f"Error reading file '{file_name}': {e}") from e
    except Exception as e: # Catch any other unexpected errors during file loading
        logger.error(f"Unexpected error loading file '{path_obj}': {e}", exc_info=True)
        raise IOError(f"Unexpected error reading file '{file_name}': {e}") from e

    if config_data is None:
        logger.error(f"Configuration file is empty: {path_obj}")
        raise ValueError(f"Configuration file '{file_name}' is empty.")

    if not isinstance(config_data, dict):
        logger.error(f"Invalid configuration format in '{path_obj}'. Expected dict, got {type(config_data).__name__}.")
        raise ValueError(
            f"Invalid configuration format in '{file_name}'. Expected a dictionary, got {type(config_data).__name__}."
        )

    logger.debug(f"YAML configuration loaded successfully from '{path_obj}'.")
    return config_data


def load_sequential_blocking_workflow_config(config_file_path: Union[str, Path]) -> SequentialBlockingWorkflow:
    """
    Carrega e valida as configurações de um workflow de linkage sequencial baseado em fases de blocking
    de um arquivo YAML, mapeando para um objeto SequentialBlockingWorkflow.

    A validação detalhada dos campos, tipos e estruturas aninhadas (como BlockingPhase, ComparisonRule)
    é delegada à função `load_workflow_from_dict` e aos respectivos dataclasses.

    Args:
        config_file_path: Caminho para o arquivo de configuração YAML do workflow.

    Returns:
        Uma instância de `SequentialBlockingWorkflow` preenchida e validada.

    Raises:
        FileNotFoundError: Se o arquivo de configuração não for encontrado.
        ValueError: Se o arquivo YAML for inválido, os dados não puderem ser mapeados
                    para `SequentialBlockingWorkflow`, ou falharem nas validações
                    internas dos dataclasses.
        IOError: Se ocorrer um erro ao ler o arquivo.

    Example:
        Exemplo de um arquivo de configuração `sequential_workflow.yaml`:

        .. code-block:: yaml

            workflow_name: "linkage_pacientes_basico"
            source_id_column: "id_paciente_source"
            candidate_id_column: "id_paciente_candidate"
            candidate_data_prefix: "cand_"
            phases:
              - phase_name: "Fase_1_Nome_e_Nascimento"
                blocking_rules:
                  - source_field: "primeiro_nome"
                    candidate_field: "primeiro_nome"
                  - source_field: "ano_nascimento"
                    candidate_field: "ano_nascimento"
                rules:
                  - source_field: "nome_completo_limpo"
                    candidate_field: "nome_completo_limpo"
                    method: "jaro_winkler"
                    weight: 0.5
                  - source_field: "data_nascimento"
                    candidate_field: "data_nascimento"
                    method: "exact"
                    weight: 0.5
                match_threshold: 0.9
    """
    file_name = Path(config_file_path).name
    logger.info(f"Loading SequentialBlockingWorkflow configuration from: {file_name}")

    # 1. Load YAML content into a dictionary using the base loader.
    try:
        config_data_dict = load_yaml(config_file_path)
    except (FileNotFoundError, ValueError, IOError) as e:
        # Log the error and re-raise to be handled by the caller
        logger.error(f"Failed to load base YAML for workflow configuration '{file_name}': {e}")
        raise

    # 2. Instantiate the SequentialBlockingWorkflow model using the helper function from models.py
    try:
        # The load_workflow_from_dict function handles the creation of nested BlockingPhase and ComparisonRule objects
        workflow_config_object = load_workflow_from_dict(config_data_dict)
    except (TypeError, ValueError) as e:
        # These errors typically indicate issues with data structure or values not matching the model
        error_msg = (f"Invalid data structure or values for SequentialBlockingWorkflow "
                     f"in file '{file_name}': {e}")
        logger.error(error_msg, exc_info=True)
        raise ValueError(error_msg) from e
    except Exception as e: # Catch any other unexpected errors during model instantiation
        error_msg = (f"Unexpected error instantiating SequentialBlockingWorkflow "
                     f"from file '{file_name}': {e}")
        logger.error(error_msg, exc_info=True)
        raise ValueError(error_msg) from e

    logger.info(f"SequentialBlockingWorkflow configuration '{workflow_config_object.workflow_name}' loaded and validated successfully from '{file_name}'.")
    return workflow_config_object

def load_column_config(config_file: Union[str, Path]) -> List[Union[ColumnConfig, ConcatenateColumnConfig]]:
    """Carrega e valida as configurações de limpeza e concatenação de colunas.

    Esta função lê um arquivo YAML que define as operações de limpeza e
    concatenação a serem aplicadas a um DataFrame. O arquivo deve conter
    uma chave `columns` com uma lista de regras de limpeza. Opcionalmente,
    pode conter uma chave `concatenate` para regras de concatenação.

    Args:
        config_file (Union[str, Path]): O caminho para o arquivo de
            configuração YAML.

    Returns:
        List[Union[ColumnConfig, ConcatenateColumnConfig]]: Uma lista de objetos
            de configuração (`ColumnConfig` e `ConcatenateColumnConfig`) que
            representam todas as operações a serem executadas.

    Raises:
        ValueError: Se a chave `columns` estiver ausente ou for inválida, ou se
            a estrutura de dados de qualquer item de configuração não
            corresponder aos dataclasses esperados.
        FileNotFoundError: Se o arquivo de configuração não for encontrado.
        IOError: Se ocorrer um erro ao ler o arquivo.

    Example:
        Exemplo de um arquivo de configuração `columns_config.yaml`:

        .. code-block:: yaml

            columns:
              - name: "nome_do_paciente"
                cleaned_name: "nome_paciente_limpo"
                to_upper: true
                remove_accents: true
                remove_special_chars: true
                remove_numbers: true
              - name: "nome_da_mae"
                cleaned_name: "nome_mae_limpo"
                to_upper: true
                remove_accents: true
                remove_special_chars: true
                remove_numbers: true

            concatenate:
              - new_column_name: "endereco_completo"
                source_columns: ["tipo_logradouro", "logradouro", "numero"]
                separator: " "
    """
    config = load_yaml(config_file)
    file_name = Path(config_file).name

    columns2clean = config.get("columns")
    if not isinstance(columns2clean, list):
        raise ValueError(f"Missing or invalid 'columns' list in configuration file '{file_name}'.")
    if not columns2clean:
        raise ValueError(f"No columns defined under 'columns' key in configuration file '{file_name}'.")

    columns_config = []
    try:
        for col in columns2clean:
            columns_config.append(ColumnConfig(**col))
    except TypeError as e:
        raise ValueError(f"Invalid data structure for an item in 'columns' in '{file_name}': {e}")

    columns2concatenate = config.get("concatenate")
    if columns2concatenate:
        if not isinstance(columns2concatenate, list):
            raise ValueError(f"Invalid 'concatenate' configuration in '{file_name}'. Expected a list.")
        try:
            for conc in columns2concatenate:
                columns_config.append(ConcatenateColumnConfig(**conc))
        except TypeError as e:
            raise ValueError(f"Invalid data structure for 'concatenate' in '{file_name}': {e}")

    return columns_config


def load_index_config(config_file_path: Union[str, Path]) -> ESIndexDefinition:
    """
    Carrega e valida as configurações de índice Elasticsearch de um arquivo YAML,
    mapeando-as para um objeto `ESIndexDefinition`.

    Args:
        config_file_path: Caminho para o arquivo de configuração YAML do índice.

    Returns:
        ESIndexDefinition: Uma instância de `ESIndexDefinition` preenchida e validada.

    Raises:
        FileNotFoundError, ValueError, IOError: Propagadas de `load_yaml` ou devido a
                                                 estrutura de configuração inválida para `ESIndexDefinition`.

    Example:
        Exemplo de um arquivo de configuração `es_index.yaml`:

        .. code-block:: yaml

            index_config:
              name: "cidacs_rlp_index"
              settings:
                number_of_shards: 5
                number_of_replicas: 1
                analysis:
                  analyzer:
                    default:
                      type: "standard"

            columns:
              - name: "id_paciente"
                type: "keyword"
              - name: "nome_completo_limpo"
                type: "text"
                analyzer: "standard"
              - name: "data_nascimento"
                type: "date"
                format: "yyyy-MM-dd"
    """
    config_dict = load_yaml(config_file_path)
    file_name = Path(config_file_path).name
    logger.info(f"Loading Elasticsearch index definition from: {file_name}")

    try:
        # Extract column definitions
        columns_data = config_dict.get("columns")
        if not isinstance(columns_data, list) or not columns_data:
            raise ValueError(f"'columns' section is missing, not a list, or empty in '{file_name}'.")
        
        column_definitions: List[ESColumnDefinition] = []
        for col_data in columns_data:
            if not isinstance(col_data, dict):
                raise ValueError(f"Invalid column definition format in 'columns' section of '{file_name}'. Expected dict, got {type(col_data)}.")
            column_definitions.append(ESColumnDefinition(**col_data))

        # Extract index settings
        index_config_data = config_dict.get("index_config")
        if not isinstance(index_config_data, dict):
            raise ValueError(f"'index_config' section is missing or not a dictionary in '{file_name}'.")

        index_name = index_config_data.pop("name", None) # Pop name as it's a direct field of ESIndexDefinition
        if not index_name:
            raise ValueError(f"'name' is missing in 'index_config' section of '{file_name}'.")

        # index_config_data now contains only fields for ESIndexSettings (e.g., shards, replicas, analysis)
        index_settings = ESIndexSettings(**index_config_data)

        # Create the ESIndexDefinition object
        index_definition = ESIndexDefinition(
            name=index_name,
            settings=index_settings,
            columns=column_definitions
        )
        logger.info(f"Elasticsearch index definition '{index_definition.name}' loaded successfully from '{file_name}'.")
        return index_definition

    except TypeError as e: # Handles issues like unexpected keys or missing required keys in dataclass instantiation
        error_msg = (f"Type error creating ESIndexDefinition from '{file_name}'. "
                     f"Check for unexpected/missing keys or incorrect types in 'columns' or 'index_config'. Error: {e}")
        logger.error(error_msg, exc_info=True)
        raise ValueError(error_msg) from e
    except ValueError as e: # Handles explicit ValueErrors raised or from dataclass validation
        error_msg = f"Invalid structure or value in index configuration file '{file_name}': {e}"
        logger.error(error_msg, exc_info=True)
        raise ValueError(error_msg) from e


def load_service_config(
    config_file_path: Union[str, Path], service_name: str = "generic"
) -> Dict[str, Any]:
    """
    Carrega configurações genéricas de serviço de um arquivo YAML.
    Esta é uma função de carregamento mais geral para arquivos de configuração
    que não precisam de validação de estrutura específica além do `load_yaml` básico.

    Args:
        config_file_path: Caminho para o arquivo de configuração YAML do serviço.
        service_name: Nome descritivo do serviço (ex: "elasticsearch", "spark") para logging.

    Returns:
        Um dicionário contendo os dados de configuração do serviço.

    Raises:
        FileNotFoundError, ValueError, IOError: Propagadas de `load_yaml`.

    Example:
        Exemplo de um arquivo de configuração `spark_config.yaml`:

        .. code-block:: yaml

            spark.driver.memory: "4g"
            spark.executor.memory: "8g"
            spark.executor.cores: 4
            spark.sql.shuffle.partitions: 200
            spark.jars.packages: "org.elasticsearch:elasticsearch-spark-30_2.12:8.4.1"

    """
    config_data = load_yaml(config_file_path)
    file_name = Path(config_file_path).name
    logger.info(
        f"Configuration for service '{service_name}' loaded successfully from '{file_name}'."
    )
    return config_data