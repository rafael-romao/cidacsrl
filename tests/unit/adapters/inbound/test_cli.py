import logging
import sys
import pytest
from unittest.mock import patch, MagicMock

from cidacsrl.adapters.inbound.cli.main import main

_MODULE = "cidacsrl.adapters.inbound.cli.main"


@pytest.fixture(autouse=True)
def _restore_root_logging_state():
    """main() chama configure_logging(), que prende um StreamHandler ao sys.stdout
    capturado pelo pytest naquele teste. Sem isso, o handler sobrevive com uma
    referência a um stream já fechado, e qualquer log emitido depois (ex.: o
    finalizador do gateway py4j no teardown da fixture Spark) explode com
    'I/O operation on closed file'."""
    original_handlers = logging.root.handlers[:]
    original_level = logging.root.level
    yield
    logging.root.handlers[:] = original_handlers
    logging.root.setLevel(original_level)


@patch(f"{_MODULE}.load_yaml")
@patch(f"{_MODULE}.build_linkage_use_case")
def test_linkage_command_executes_and_stops_spark(mock_build, mock_load_yaml):
    env_data = {
        "storage": {"source_path": "data/a.parquet", "target_path": "data/b.parquet"},
        "execution": {"job_id": "job1"},
        "elasticsearch": {"es_connection_url": "http://localhost:9200"},
        "spark": {},
        "specification": {},
    }
    spec_data = {"fields": []}
    mock_load_yaml.side_effect = lambda p: env_data if p == "env.yml" else spec_data

    mock_use_case = MagicMock()
    mock_enriched = MagicMock()
    mock_spark = MagicMock()
    mock_build.return_value = (mock_use_case, MagicMock(), mock_enriched, mock_spark)

    with patch.object(sys, "argv", ["cidacsrl", "linkage", "--env-config", "env.yml", "--spec-config", "spec.yml"]):
        main()

    mock_build.assert_called_once_with(
        storage_config_data=env_data["storage"],
        execution_config_data=env_data["execution"],
        linkage_spec_data=spec_data,
        es_config_data=env_data["elasticsearch"],
        spark_config_data=env_data["spark"],
    )
    mock_use_case.execute.assert_called_once()
    mock_spark.stop.assert_called_once()


@patch(f"{_MODULE}.load_yaml")
@patch(f"{_MODULE}.build_linkage_use_case")
def test_linkage_resolves_spec_path_from_env_yaml(mock_build, mock_load_yaml):
    spec_path = "specs/linkage.yml"
    env_data = {
        "storage": {}, "execution": {}, "elasticsearch": {}, "spark": {},
        "specification": {"linkage_path": spec_path},
    }
    spec_data = {"fields": []}
    mock_load_yaml.side_effect = lambda p: env_data if p == "env.yml" else spec_data

    mock_spark = MagicMock()
    mock_build.return_value = (MagicMock(), MagicMock(), MagicMock(), mock_spark)

    with patch.object(sys, "argv", ["cidacsrl", "linkage", "--env-config", "env.yml"]):
        main()

    mock_load_yaml.assert_any_call(spec_path)
    mock_spark.stop.assert_called_once()


@patch(f"{_MODULE}.load_yaml")
@patch(f"{_MODULE}.build_indexing_use_case")
def test_indexing_command_executes_and_stops_spark(mock_build, mock_load_yaml):
    env_data = {
        "storage": {}, "execution": {"audit_log_path": "tests/data/audit_logs"}, "elasticsearch": {}, "spark": {},
        "specification": {},
    }
    spec_data = {"dataset": "births"}
    mock_load_yaml.side_effect = lambda p: env_data if p == "env.yml" else spec_data

    mock_use_case = MagicMock()
    mock_spark = MagicMock()
    mock_build.return_value = (mock_use_case, MagicMock(), mock_spark)

    with patch.object(sys, "argv", ["cidacsrl", "indexing", "--env-config", "env.yml", "--spec-config", "spec.yml"]):
        main()

    mock_build.assert_called_once_with(
        storage_config_data=env_data["storage"],
        execution_config_data=env_data["execution"],
        indexing_spec_data=spec_data,
        es_config_data=env_data["elasticsearch"],
        spark_config_data=env_data["spark"],
    )
    mock_use_case.execute.assert_called_once()
    mock_spark.stop.assert_called_once()


@patch(f"{_MODULE}.build_deduplication_use_case")
@patch(f"{_MODULE}.load_deduplicate_workflow_config")
def test_deduplication_command_executes_and_stops_spark(mock_loader, mock_build):
    mock_config = MagicMock()
    mock_loader.return_value = mock_config
    mock_use_case = MagicMock()
    mock_spark = MagicMock()
    mock_build.return_value = (mock_use_case, mock_spark)

    with patch.object(sys, "argv", ["cidacsrl", "deduplication", "--config-path", "dedup.yml"]):
        main()

    mock_loader.assert_called_once_with("dedup.yml")
    mock_build.assert_called_once_with(mock_config)
    mock_use_case.execute.assert_called_once_with(spec=mock_config.deduplication_spec)
    mock_spark.stop.assert_called_once()


@patch(f"{_MODULE}.load_deduplicate_workflow_config")
def test_deduplication_exits_with_1_on_loader_error(mock_loader):
    mock_loader.side_effect = FileNotFoundError("não encontrado")

    with patch.object(sys, "argv", ["cidacsrl", "deduplication", "--config-path", "missing.yml"]):
        with pytest.raises(SystemExit) as exc_info:
            main()

    assert exc_info.value.code == 1


@patch(f"{_MODULE}.build_linkage_use_case")
@patch(f"{_MODULE}.load_yaml")
def test_linkage_exits_with_1_on_execute_error(mock_load_yaml, mock_build):
    env_data = {"storage": {}, "execution": {}, "elasticsearch": {}, "spark": {}, "specification": {}}
    mock_load_yaml.side_effect = lambda p: env_data if p == "env.yml" else {}

    mock_use_case = MagicMock()
    mock_spark = MagicMock()
    mock_build.return_value = (mock_use_case, MagicMock(), MagicMock(), mock_spark)
    mock_use_case.execute.side_effect = RuntimeError("falha simulada")

    with patch.object(sys, "argv", ["cidacsrl", "linkage", "--env-config", "env.yml", "--spec-config", "spec.yml"]):
        with pytest.raises(SystemExit) as exc_info:
            main()

    assert exc_info.value.code == 1
    mock_spark.stop.assert_called_once()


def test_missing_subcommand_exits():
    with patch.object(sys, "argv", ["cidacsrl"]):
        with pytest.raises(SystemExit):
            main()


def test_unknown_subcommand_exits():
    with patch.object(sys, "argv", ["cidacsrl", "cleaning", "--env-config", "e.yml"]):
        with pytest.raises(SystemExit):
            main()
