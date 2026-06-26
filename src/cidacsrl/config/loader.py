import logging
from pathlib import Path
from typing import Any, Dict, List, Union

import yaml

from cidacsrl.adapters.outbound.elasticsearch.service_config import (
    ElasticsearchConfig,
)
from cidacsrl.config.models.execution_config import (
    DataPartitioningConfig,
    ExecutionConfig,
)
from cidacsrl.config.models.indexed_dataset_filter import (
    parse_indexed_dataset_filter,
)
from cidacsrl.config.models.storage_config import (
    OutputStorageConfig,
    SourceStorageConfig,
)
from cidacsrl.domain.indexing.indexing_specification import (
    DatasetIndexingSpecification,
)
from cidacsrl.domain.linkage.linkage_specification import (
    SequentialLinkageSpecification,
)

logger = logging.getLogger("Loader: Configuration Loader")

def load_yaml(file_path: Union[str, Path]) -> Dict[str, Any]:
    """Carrega e valida um arquivo de configuração YAML.

    Args:
        file_path: Caminho para o arquivo YAML.

    Returns:
        Dicionário com o conteúdo do YAML; vazio se o arquivo estiver vazio.

    Raises:
        FileNotFoundError: Se o arquivo não existir.
        ValueError: Se o caminho não for um arquivo válido, não tiver extensão YAML,
            tiver erro de sintaxe ou o conteúdo não for um dicionário.
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
        raise ValueError(f"File '{file_name}' must be a valid YAML file (.yml or .yaml).")
        
    with open(path_obj, "r", encoding="utf-8") as f:
        try:
            content = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file '{file_name}': {e}") from e

    if content is None:
        return {}
    if not isinstance(content, dict):
        raise ValueError(f"YAML file '{file_name}' content must be a dictionary.")
        
    return content


def parse_source_storage_config(data: Dict[str, Any]) -> SourceStorageConfig:
    """Constrói SourceStorageConfig a partir de um dicionário de configuração."""
    return SourceStorageConfig.from_dict(data)


def parse_output_storage_config(data: Dict[str, Any]) -> OutputStorageConfig:
    """Constrói OutputStorageConfig a partir de um dicionário de configuração."""
    return OutputStorageConfig.from_dict(data)


def parse_execution_config(data: Dict[str, Any]) -> ExecutionConfig:
    """Constrói ExecutionConfig a partir de um dicionário de configuração.

    Aceita tanto um dicionário com chave 'execution' quanto os campos diretamente.

    Args:
        data: Dicionário com configuração de execução.

    Returns:
        ExecutionConfig com job_id, particionamento, amostragem e path de auditoria.
    """
    exec_data = data.get("execution", {}) if "execution" in data else data
    
    partition_data = exec_data.get("partitioning", {})
    partitioning = DataPartitioningConfig(
        partition_column=partition_data.get("partition_column"),
        filter_partitions=partition_data.get("filter_partitions", [])
    )
    
    return ExecutionConfig(
        job_id=exec_data.get("job_id"),
        partitioning=partitioning,
        sample_fraction=exec_data.get("sample_fraction"),
        sample_seed=exec_data.get("sample_seed", 42),
        audit_log_path=exec_data.get("audit_log_path")
    )


def parse_es_config(data: Dict[str, Any]) -> ElasticsearchConfig:
    """Traduz o bloco 'elasticsearch' para ElasticsearchConfig com validação fail-fast.

    Args:
        data: Dicionário com configuração do Elasticsearch.

    Returns:
        ElasticsearchConfig preenchida com os valores do dicionário.

    Raises:
        ValueError: Se o bloco estiver ausente ou 'es_connection_url' não estiver presente.
    """
    if not data:
        raise ValueError("O bloco de configuração 'elasticsearch' está ausente no arquivo de ambiente.")
    
    if "es_connection_url" not in data:
        raise ValueError("A propriedade 'es_connection_url' é obrigatória dentro do bloco elasticsearch.")

    return ElasticsearchConfig(
        es_connection_url=data["es_connection_url"],
        verify_certs=data.get("verify_certs", True),
        request_timeout=data.get("request_timeout", 30),
        msearch_batch_size=data.get("msearch_batch_size", 100),
        es_user=data.get("es_user"),
        es_password=data.get("es_password"),
        api_key=data.get("api_key"),
        search_strategy=data.get("search_strategy", "multisearch")
    )


def parse_sequential_linkage_specification(data: Dict[str, Any]) -> SequentialLinkageSpecification:
    """Constrói SequentialLinkageSpecification a partir de um dicionário de configuração."""
    return SequentialLinkageSpecification.from_dict(data)


def parse_dataset_indexing_specification(data: Dict[str, Any]) -> DatasetIndexingSpecification:
    """Valida e constrói DatasetIndexingSpecification a partir de um dicionário.

    Args:
        data: Dicionário com 'source_config', 'index_config' e 'index_columns'.

    Returns:
        DatasetIndexingSpecification validada e pronta para uso.

    Raises:
        ValueError: Se 'source_config.id_field' estiver ausente, shards/replicas forem
            inválidos, ou 'index_columns' estiver ausente ou incompleta.
    """
    source_cfg = data.get("source_config")
    if not source_cfg or "id_field" not in source_cfg:
        raise ValueError("O 'source_config' deve ter 'id_field' definido para indicar o campo de ID no Elasticsearch.")    
    
    idx_cfg = data.get("index_config", {})
    if idx_cfg.get("number_of_shards", 1) <= 0:
        raise ValueError("O 'number_of_shards' deve ser um número inteiro positivo.")
    if idx_cfg.get("number_of_replicas", 0) < 0:
        raise ValueError("O 'number_of_replicas' não pode ser um valor negativo.")

    col_cfgs = data.get("index_columns")
    if not isinstance(col_cfgs, list) or len(col_cfgs) == 0:
        raise ValueError("O 'index_columns' deve ser uma lista com as definições das colunas a serem indexadas.")
    for col in col_cfgs:
        if "name" not in col or "type" not in col:
            raise ValueError(f"Toda coluna deve conter obrigatoriamente 'name' e 'type'. Mapeamento incorreto: {col}")

    return DatasetIndexingSpecification.from_dict(data)


def load_sequential_linkage_specification(path: Union[str, Path]) -> SequentialLinkageSpecification:
    """Carrega e parseia uma especificação de linkage a partir de um arquivo YAML.

    Args:
        path: Caminho para o arquivo YAML de especificação de linkage.

    Returns:
        SequentialLinkageSpecification construída a partir do arquivo.
    """
    return parse_sequential_linkage_specification(load_yaml(path))


def load_dataset_indexing_specification(path: Union[str, Path]) -> DatasetIndexingSpecification:
    """Carrega e parseia uma especificação de indexação a partir de um arquivo YAML.

    Args:
        path: Caminho para o arquivo YAML de especificação de indexação.

    Returns:
        DatasetIndexingSpecification construída a partir do arquivo.
    """
    return parse_dataset_indexing_specification(load_yaml(path))