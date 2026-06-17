import os
from pathlib import Path
import pytest
from core.infra.configs.logging_config import configure_logging
from unittest.mock import MagicMock

configure_logging()


def pytest_collection_modifyitems(items):
    for item in items:
        path_str = str(item.fspath)
        if "/unit/" in path_str:
            item.add_marker(pytest.mark.unit)
        elif "/integration/" in path_str:
            item.add_marker(pytest.mark.integration)
        elif "/e2e/" in path_str:
            item.add_marker(pytest.mark.e2e)

@pytest.fixture(scope="session")
def test_paths():
    """Centraliza as rotas absolutas do ambiente de teste para evitar caminhos quebrados."""
    tests_root = Path(__file__).parent.resolve()  # cidacsrl_rlp/tests/
    
    return {
        "input_data":       tests_root / "data" / "input",
        "output_data":      tests_root / "data" / "output",
        "configs":          tests_root / "configs",
        "spark_config":     tests_root / "configs" / "spark_local.yml",
        "linkage_spec_e2e": tests_root / "configs" / "linkage_spec_e2e.yml",
    }


@pytest.fixture(scope="session")
def es_url() -> str:
    return os.environ.get("CIDACSRL_ES_URL", "http://localhost:9200")

@pytest.fixture(scope="session")
def es_config_data(es_url) -> dict:
    return {"es_connection_url": es_url, "wan_only": True}

@pytest.fixture(scope="session")
def storage_config_data(test_paths) -> dict:
    return {
        "source_data_path": str(test_paths["input_data"]),
        "output_data_path": str(test_paths["output_data"]),
        "source_data_format": "parquet",
        "output_data_format": "parquet",
    }


@pytest.fixture
def mock_env_yaml_content() -> dict:
    return {
        "storage": {
            "source_path": "tests/data/input",
            "source_format": "parquet",
            "output_path": "tests/data/output",
            "output_format": "parquet"
        },
        "execution": {
            "sample_fraction": 0.1,
            "sample_seed": 42
        },
        "specification": {
            "indexing_path": "tests/configs/specifications/indexing_default.yml",
            "linkage_path": "tests/configs/specifications/linkage_default.yml"
        },
        "spark": {
            "spark_configs": {"spark.master": "local[*]"}
        },
        "elasticsearch": {
            "es_connection_url": "http://localhost:9200",
            "search_strategy": "multisearch"
        }
    }


@pytest.fixture
def mock_spec_yaml_content() -> dict:
    return {"source_table": "tabela_origem", "target_es_index": "indice_destino"}