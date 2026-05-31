import pytest
from unittest.mock import patch, mock_open
from pathlib import Path

from cidacsrl_rlp.cidacsrl.infra.configs.loader import (
    load_yaml,
    load_linkage_env_config,
    load_sequential_linkage_specification,
    load_es_config,
    parse_dataset_indexing_specification,
    load_dataset_indexing_specification,
    parse_es_config
)

from cidacsrl_rlp.cidacsrl.infra.configs.models.linkage_env_config import LinkageEnvironmentConfig
from cidacsrl_rlp.cidacsrl.domain.models.linkage_specification import SequentialLinkageSpecification
from cidacsrl_rlp.cidacsrl.domain.models.indexing_specification import DatasetIndexingSpecification

# =========================================================================
# 1. CASOS DE USO: FLUXO BASE DE LEITURA (load_yaml)
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
        assert "Expected a dictionary, got list" in str(exc_info.value)

# =========================================================================
# 2. CASOS DE USO: ATUALIZAÇÃO DA INDEXAÇÃO
# =========================================================================

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
            {"name": "nome_completo", "type": "text", "index_as": "both"}
        ]
    }
    result = parse_dataset_indexing_specification(valid_indexing_data)
    assert isinstance(result, DatasetIndexingSpecification)
    assert result.index_config.name == "nascimentos_test"
    assert result.index_config.number_of_shards == 2
    assert len(result.index_columns) == 2
    assert result.index_columns[1].index_as == "both"

@pytest.mark.parametrize("missing_key, error_msg", [
    ("source_config", "O campo 'source_config' com as configurações dos dados de origem é obrigatório."),
    ("index_config", "O campo 'index_config' com as configurações do índice é obrigatório.")
])
def test_parse_indexing_missing_nodes(missing_key, error_msg):
    base_data = {
        "source_config": {"source_table": "tabela", "id_field": "id"},
        "index_config": {
            "name": "test"
        },
        "index_columns": [{"name": "id", "type": "keyword"}]
    }
    base_data.pop(missing_key)
    with pytest.raises(ValueError) as exc_info:
        parse_dataset_indexing_specification(base_data)
    assert error_msg in str(exc_info.value)

@pytest.mark.parametrize("invalid_config, error_msg", [
    ({"index_config": {"name": ""}}, "O 'index_config' deve ter um campo 'name' para definir o nome do index."),
    ({"index_config": {"name": "a", "number_of_shards": 0}}, "O 'number_of_shards' deve ser um numero inteiro positivo."),
    ({"index_config": {"name": "a", "number_of_replicas": -1}}, "O 'number_of_replicas' nao pode ser um valor negativo.")
])
def test_parse_indexing_invalid_sanity_parameters(invalid_config, error_msg):
    full_payload = {
        "source_config": {"source_table": "tabela", "id_field": "id"},
        "index_config": {"name": "a", "number_of_shards": 1, "number_of_replicas": 0},
        "index_columns": [{"name": "id", "type": "keyword"}]
    }
    if "index_config" in invalid_config:
        full_payload["index_config"].update(invalid_config["index_config"])
    with pytest.raises(ValueError) as exc_info:
        parse_dataset_indexing_specification(full_payload)
    assert error_msg in str(exc_info.value)

def test_parse_indexing_column_missing_attributes():
    invalid_column_data = {
        "source_config": {"source_table": "tabela", "id_field": "id"},
        "index_config": {
            "name": "nascimentos"
        },
        "index_columns": [
            {"name": "so_tem_o_name_sem_o_type"}
        ]
    }
    with pytest.raises(ValueError) as exc_info:
        parse_dataset_indexing_specification(invalid_column_data)
    assert "Toda coluna deve conter obrigatoriamente 'name' e 'type'" in str(exc_info.value)

def test_load_dataset_indexing_specification_integration():
    mock_yaml_data = {
        "source_config": {
            "source_table": "tabela_origem",
            "id_field": "id"
        },
        "index_config": {
            "name": "nascimentos_test",
            "number_of_shards": 2,
            "number_of_replicas": 0,
            "refresh_interval": "1s"
        },
        "index_columns": [
            {"name": "codigo_nascimento", "type": "keyword"},
            {"name": "nome_completo", "type": "text", "index_as": "both"}
        ]
    }
    with patch("cidacsrl_rlp.cidacsrl.infra.configs.loader.load_yaml", return_value=mock_yaml_data):
        result = load_dataset_indexing_specification("any_path.yml")
        assert isinstance(result, DatasetIndexingSpecification)
        assert result.index_config.name == "nascimentos_test"
        assert len(result.index_columns) == 2

# =========================================================================
# 3. CASOS DE USO: CONFIGURAÇÃO DE AMBIENTE E LINKAGE
# =========================================================================

def test_parse_es_config_success():
    es_data = {
        "es_connection_url": "https://localhost:9200",
        "msearch_batch_size": 250,
        "request_timeout": 30
    }
    resolved_config = parse_es_config(es_data)
    assert "msearch_batch_size" in resolved_config
    assert resolved_config["msearch_batch_size"] == 250

@pytest.mark.parametrize("invalid_es_payload, expected_error", [
    ({"request_timeout": 0}, "A configuração do Elasticsearch deve conter 'es_connection_url' ou 'cloud_id'."),
    ({"es_connection_url": "ftp://invalid-protocol.com"}, "'es_connection_url' inválida"),
    ({"es_connection_url": "http://ok.com", "msearch_batch_size": -5}, "'msearch_batch_size' deve ser um valor positivo")
])
def test_parse_es_config_failures(invalid_es_payload, expected_error):
    with pytest.raises(ValueError) as exc_info:
        parse_es_config(invalid_es_payload)
    assert expected_error in str(exc_info.value)

def test_load_linkage_env_config_integration():
    mock_yaml_data = {
        "source_data_path": "/data/source",
        "output_data_path": "/data/output",
        "linkage_specification_path": "/configs/spec.yml",
        "es_config_path": "/configs/es.yml",
        "spark_config_path": "/configs/spark.yml",
        "source_data_format": "parquet",
        "output_data_format": "parquet"
    }
    with patch("cidacsrl_rlp.cidacsrl.infra.configs.loader.load_yaml", return_value=mock_yaml_data):
        result = load_linkage_env_config("any_path.yml")
        assert isinstance(result, LinkageEnvironmentConfig)
        assert result.source_data_path == "/data/source"

def test_load_sequential_linkage_specification_integration():
    mock_linkage_spec = {
        "source_table": "origem_test",
        "target_es_index": "target_test",
        "id_source_table": "id",
        "id_target_table": "id",
        "blocking_phases": [
            {
                "phase_name": "Phase_1",
                "enabled": True,
                "candidate_limit": 100,
                "strong_match_score_threshold": 0.90,
                "rules": []
            }
        ]
    }
    with patch("cidacsrl_rlp.cidacsrl.infra.configs.loader.load_yaml", return_value=mock_linkage_spec):
        result = load_sequential_linkage_specification("any_path.yml")
        assert isinstance(result, SequentialLinkageSpecification)
        assert result.source_table == "origem_test"
        assert len(result.blocking_phases) == 1

def test_load_es_config_integration():
    mock_yaml_data = {
        "es_connection_url": "https://localhost:9200",
        "msearch_batch_size": 250,
        "request_timeout": 30
    }
    with patch("cidacsrl_rlp.cidacsrl.infra.configs.loader.load_yaml", return_value=mock_yaml_data):
        result = load_es_config("any_path.yml")
        assert "msearch_batch_size" in result
        assert result["msearch_batch_size"] == 250