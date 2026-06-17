import pytest
from core.domain.models.indexing_specification import (
    SourceConfig, IndexSettingsConfig, IndexColumnConfig, DatasetIndexingSpecification
)

def test_source_config_from_dict():
    data = {"source_table": "table1", "id_field": "id"}
    config = SourceConfig.from_dict(data)
    assert config.source_table == "table1"
    assert config.id_field == "id"

def test_index_settings_config_from_dict_defaults():
    data = {"name": "idx", "source_table": "table1"}
    config = IndexSettingsConfig.from_dict(data)
    assert config.name == "idx"
    assert config.id_from_source is False
    assert config.number_of_shards == 1
    assert config.number_of_replicas == 0
    assert config.refresh_interval == "1s"

def test_index_settings_config_from_dict_all_fields():
    data = {
        "name": "idx",
        "source_table": "table1",
        "id_from_source": True,
        "number_of_shards": 3,
        "number_of_replicas": 2,
        "refresh_interval": "10s"
    }
    config = IndexSettingsConfig.from_dict(data)
    assert config.id_from_source is True
    assert config.number_of_shards == 3
    assert config.number_of_replicas == 2
    assert config.refresh_interval == "10s"

def test_index_column_config_from_dict():
    data = {"name": "col1", "type": "keyword", "index_as": "text"}
    col = IndexColumnConfig.from_dict(data)
    assert col.name == "col1"
    assert col.type == "keyword"
    assert col.index_as == "text"

def test_index_column_config_from_dict_without_optional():
    data = {"name": "col1", "type": "keyword"}
    col = IndexColumnConfig.from_dict(data)
    assert col.index_as is None

def test_dataset_indexing_specification_from_dict_empty_columns():
    data = {
        "source_config": {"source_table": "table1", "id_field": "id"},
        "index_config": {"name": "idx", "source_table": "table1"}
    }
    spec = DatasetIndexingSpecification.from_dict(data)
    assert spec.index_columns == []

def test_source_config_from_dict_missing_field():
    data = {"source_table": "table1"}
    with pytest.raises(KeyError):
        SourceConfig.from_dict(data)