import pytest
from unittest.mock import patch, mock_open
from pathlib import Path

from cidacsrl.config.loader import (
    load_yaml,
    parse_source_storage_config,
    parse_output_storage_config,
    parse_execution_config,
    parse_es_config,
    parse_sequential_linkage_specification,
    load_sequential_linkage_specification,
    parse_dataset_indexing_specification,
    load_dataset_indexing_specification
)

from cidacsrl.config.models.storage_config import SourceStorageConfig, OutputStorageConfig
from cidacsrl.config.models.execution_config import ExecutionConfig
from cidacsrl.domain.linkage.linkage_specification import SequentialLinkageSpecification
from cidacsrl.domain.indexing.indexing_specification import DatasetIndexingSpecification

# =========================================================================
# 1. FLUXO BASE DE LEITURA (load_yaml)
# =========================================================================

def test_load_yaml_success():
    yaml_content = """
    key: value
    nested:
      item: 10
    """
    with patch("builtins.open", mock_open(read_data=yaml_content)), \
         patch.object(Path, "exists", return_value=True), \
         patch.object(Path, "is_file", return_value=True):
        result = load_yaml("dummy_path.yml")
        assert result == {"key": "value", "nested": {"item": 10}}

def test_load_yaml_file_not_found():
    with patch.object(Path, "exists", return_value=False):
        with pytest.raises(FileNotFoundError) as exc_info:
            load_yaml("missing_file.yml")
        assert "not found at" in str(exc_info.value)

def test_load_yaml_invalid_format():
    yaml_content = "- apenas uma lista pura\n- nao eh dicionario"
    with patch("builtins.open", mock_open(read_data=yaml_content)), \
         patch.object(Path, "exists", return_value=True), \
         patch.object(Path, "is_file", return_value=True):
        with pytest.raises(ValueError) as exc_info:
            load_yaml("invalid_structure.yml")
        assert "content must be a dictionary" in str(exc_info.value)


# =========================================================================
# 2. CONFIGURAÇÃO DE AMBIENTE (Mapeamento do Novo env_local.yml)
# =========================================================================

def test_parse_source_storage_config_success():
    mock_storage_data = {
        "source_path": "tests/data/input",
        "source_format": "parquet",
        "output_path": "tests/data/output",
        "output_format": "parquet"
    }
    result = parse_source_storage_config(mock_storage_data)
    assert isinstance(result, SourceStorageConfig)
    assert result.source_path == "tests/data/input"
    assert result.source_format == "parquet"

def test_parse_output_storage_config_success():
    mock_storage_data = {
        "source_path": "tests/data/input",
        "source_format": "parquet",
        "output_path": "tests/data/output",
        "output_format": "csv"
    }
    result = parse_output_storage_config(mock_storage_data)
    assert isinstance(result, OutputStorageConfig)
    assert result.output_path == "tests/data/output"
    assert result.output_format == "csv"

def test_parse_storage_config_missing_node_raises_error():
    with pytest.raises(ValueError, match="'source_path' é obrigatório"):
        parse_source_storage_config({})

def test_parse_execution_config_completo():
    mock_execution_data = {
        "sample_fraction": 0.25,
        "sample_seed": 1337,
        "audit_log_path": "tests/data/audit",
        "partitioning": {
            "partition_column": "uf_internacao",
            "filter_partitions": ["BA", "SP"]
        }
    }
    result = parse_execution_config(mock_execution_data)
    assert isinstance(result, ExecutionConfig)
    assert result.sample_fraction == 0.25
    assert result.sample_seed == 1337
    assert result.audit_log_path == "tests/data/audit"
    assert result.partitioning.partition_column == "uf_internacao"
    assert result.partitioning.filter_partitions == ["BA", "SP"]
    assert result.partitioning.has_filters is True

def test_parse_execution_config_vazio_usa_defaults():
    result = parse_execution_config({})
    assert isinstance(result, ExecutionConfig)
    assert result.sample_fraction is None
    assert result.sample_seed == 42
    assert result.partitioning.partition_column is None
    assert result.partitioning.has_filters is False

def test_parse_es_config_success():
    es_data = {
        "es_connection_url": "http://localhost:9200",
        "search_strategy": "multisearch",
        "msearch_batch_size": 250,
        "request_timeout": 30
    }
    resolved_config = parse_es_config(es_data)
    assert resolved_config["es_connection_url"] == "http://localhost:9200"
    assert resolved_config["search_strategy"] == "multisearch"
    assert resolved_config["msearch_batch_size"] == 250

def test_parse_es_config_missing_url_raises_error():
    invalid_es_data = {
        "host": "localhost",
        "port": 9200
    }
    with pytest.raises(ValueError, match="es_connection_url' é obrigatória"):
        parse_es_config(invalid_es_data)


# =========================================================================
# 3. ESPECIFICAÇÕES DE WORKFLOW (Domínio)
# =========================================================================

def test_load_sequential_linkage_specification_integration():
    mock_linkage_spec = {
        "source_table": "internacao_example",
        "target_es_index": "nascimentos_example_index",
        "id_source_table": "codigo_internacao",
        "id_target_table": "codigo_nascimento",
        "blocking_phases": [
            {
                "phase_name": "fase_teste",
                "enabled": True,
                "candidate_limit": 100,
                "strong_match_score_threshold": 0.90,
                "rules": []
            }
        ]
    }
    with patch("cidacsrl.config.loader.load_yaml", return_value=mock_linkage_spec):
        result = load_sequential_linkage_specification("any_path.yml")
        assert isinstance(result, SequentialLinkageSpecification)
        assert result.source_table == "internacao_example"
        assert len(result.blocking_phases) == 1

def test_parse_indexing_specification_success():
    valid_indexing_data = {
        "source_config": {
            "source_table": "tabela_origem",
            "id_field": "id"
        },
        "index_config": {
            "name": "nascimentos_test",
            "number_of_shards": 2,
            "number_of_replicas": 0,
            "refresh_interval": "1s",
        },
        "index_columns": [
            {"name": "codigo_nascimento", "type": "keyword"},
            {"name": "nome_completo", "type": "text"}
        ]
    }
    result = parse_dataset_indexing_specification(valid_indexing_data)
    assert isinstance(result, DatasetIndexingSpecification)
    assert result.index_config.name == "nascimentos_test"
    assert result.index_config.number_of_shards == 2
    assert len(result.index_columns) == 2

def test_parse_indexing_missing_id_field_raises_error():
    invalid_data = {
        "source_config": {"source_table": "tabela"}  # Sem id_field
    }
    with pytest.raises(ValueError, match="id_field' definido"):
        parse_dataset_indexing_specification(invalid_data)

@pytest.mark.parametrize("invalid_config, error_msg", [
    ({"index_config": {"number_of_shards": 0}}, "number_of_shards' deve ser um número inteiro positivo"),
    ({"index_config": {"number_of_replicas": -1}}, "number_of_replicas' não pode ser um valor negativo")
])
def test_parse_indexing_invalid_sanity_parameters(invalid_config, error_msg):
    full_payload = {
        "source_config": {"source_table": "tabela", "id_field": "id"},
        "index_config": {"number_of_shards": 1, "number_of_replicas": 0},
        "index_columns": [{"name": "id", "type": "keyword"}]
    }
    full_payload["index_config"].update(invalid_config["index_config"])
    with pytest.raises(ValueError) as exc_info:
        parse_dataset_indexing_specification(full_payload)
    assert error_msg in str(exc_info.value)

def test_parse_indexing_column_missing_attributes():
    invalid_column_data = {
        "source_config": {"source_table": "tabela", "id_field": "id"},
        "index_config": {"name": "nascimentos"},
        "index_columns": [
            {"name": "coluna_sem_tipo"}  # Sem chave 'type'
        ]
    }
    with pytest.raises(ValueError, match="obrigatoriamente 'name' e 'type'"):
        parse_dataset_indexing_specification(invalid_column_data)

def test_load_dataset_indexing_specification_integration():
    mock_yaml_data = {
        "source_config": {"source_table": "tabela", "id_field": "id"},
        "index_config": {"name": "nascimentos_test"},
        "index_columns": [{"name": "id", "type": "keyword"}]
    }
    with patch("cidacsrl.config.loader.load_yaml", return_value=mock_yaml_data):
        result = load_dataset_indexing_specification("any_path.yml")
        assert isinstance(result, DatasetIndexingSpecification)
        assert result.index_config.name == "nascimentos_test"