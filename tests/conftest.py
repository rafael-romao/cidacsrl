import os
from pathlib import Path

import pytest

TESTS_ROOT = Path(__file__).parent.resolve()


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
    return {
        "input":      TESTS_ROOT / "fixtures" / "data",
        "env":        TESTS_ROOT / "fixtures" / "configs" / "env",
        "specs":      TESTS_ROOT / "fixtures" / "configs" / "specs",
        "output":     TESTS_ROOT / "output",
        "audit_logs": TESTS_ROOT / "audit_logs",
    }


@pytest.fixture(scope="session")
def linkage_output(test_paths):
    path = test_paths["output"] / "linkage"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture(scope="session")
def dedup_input(linkage_output):
    return linkage_output


@pytest.fixture(scope="session")
def es_url() -> str:
    return os.environ.get("CIDACSRL_ES_URL", "http://localhost:9200")


@pytest.fixture(scope="session")
def es_config_data(es_url) -> dict:
    return {"es_connection_url": es_url, "wan_only": True}
