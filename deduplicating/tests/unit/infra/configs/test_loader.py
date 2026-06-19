import pytest
from pathlib import Path
from unittest.mock import patch

from cidacsrl.config.dedup_loader import load_deduplicate_workflow_config
from cidacsrl.config.models.dedup_workflow_config import DeduplicateWorkflowConfig

pytestmark = pytest.mark.unit


def _write_yaml(tmp_path: Path, content: str) -> Path:
    f = tmp_path / "config.yml"
    f.write_text(content, encoding="utf-8")
    return f


def test_load_valid_config_returns_workflow_config(tmp_path, valid_workflow_config_data):
    yaml_content = """
app_name: "Test Deduplication"
storage:
  source_path: "data/linked.parquet"
  output_path: "data/deduped.parquet"
spark:
  spark_configs:
    spark.master: "local[*]"
deduplication:
  id_source_column: "id_table"
  id_target_column: "candidate_id_table"
"""
    path = _write_yaml(tmp_path, yaml_content)
    config = load_deduplicate_workflow_config(path)

    assert isinstance(config, DeduplicateWorkflowConfig)
    assert config.source_storage.source_path == "data/linked.parquet"
    assert config.output_storage.output_path == "data/deduped.parquet"
    assert config.deduplication_spec.id_source_column == "id_table"
    assert config.deduplication_spec.id_target_column == "candidate_id_table"
    assert config.spark_configs == {"spark.master": "local[*]"}


def test_load_raises_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_deduplicate_workflow_config("/caminho/inexistente/config.yml")


def test_load_raises_on_invalid_yaml(tmp_path):
    path = _write_yaml(tmp_path, "key: [broken yaml")
    with pytest.raises(ValueError, match="Erro ao parsear"):
        load_deduplicate_workflow_config(path)


def test_load_raises_on_missing_storage_block(tmp_path):
    path = _write_yaml(tmp_path, "deduplication:\n  id_source_column: src\n  id_target_column: dst\n")
    with pytest.raises(ValueError, match="'storage' é obrigatório"):
        load_deduplicate_workflow_config(path)


def test_load_raises_on_missing_deduplication_block(tmp_path):
    path = _write_yaml(tmp_path, "storage:\n  source_path: x\n  output_path: y\n")
    with pytest.raises(ValueError, match="'deduplication' é obrigatório"):
        load_deduplicate_workflow_config(path)


def test_load_raises_on_non_yaml_extension(tmp_path):
    f = tmp_path / "config.json"
    f.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="YAML válido"):
        load_deduplicate_workflow_config(f)
