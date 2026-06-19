import sys
import pytest
from unittest.mock import MagicMock, patch

from deduplicating.infra.adapters.inbound.cli import main

pytestmark = pytest.mark.unit

_MODULE = "deduplicating.infra.adapters.inbound.cli"


@patch(f"{_MODULE}.build_deduplication_use_case")
@patch(f"{_MODULE}.load_deduplicate_workflow_config")
def test_main_calls_loader_and_builder(mock_loader, mock_build):
    mock_config = MagicMock()
    mock_loader.return_value = mock_config
    mock_use_case = MagicMock()
    mock_spark = MagicMock()
    mock_build.return_value = (mock_use_case, mock_spark)

    with patch.object(sys, "argv", ["cli.py", "--config-path", "config.yml"]):
        main()

    mock_loader.assert_called_once_with("config.yml")
    mock_build.assert_called_once_with(mock_config)
    mock_use_case.execute.assert_called_once_with(spec=mock_config.deduplication_spec)
    mock_spark.stop.assert_called_once()


def test_main_exits_when_config_path_is_missing():
    with patch.object(sys, "argv", ["cli.py"]):
        with pytest.raises(SystemExit):
            main()


@patch(f"{_MODULE}.load_deduplicate_workflow_config")
def test_main_exits_with_code_1_on_loader_file_not_found(mock_loader):
    mock_loader.side_effect = FileNotFoundError("arquivo não encontrado")

    with patch.object(sys, "argv", ["cli.py", "--config-path", "missing.yml"]):
        with pytest.raises(SystemExit) as exc_info:
            main()

    assert exc_info.value.code == 1


@patch(f"{_MODULE}.load_deduplicate_workflow_config")
def test_main_exits_with_code_1_on_loader_value_error(mock_loader):
    mock_loader.side_effect = ValueError("YAML inválido")

    with patch.object(sys, "argv", ["cli.py", "--config-path", "bad.yml"]):
        with pytest.raises(SystemExit) as exc_info:
            main()

    assert exc_info.value.code == 1


@patch(f"{_MODULE}.build_deduplication_use_case")
@patch(f"{_MODULE}.load_deduplicate_workflow_config")
def test_main_exits_with_code_1_on_execute_error(mock_loader, mock_build):
    mock_loader.return_value = MagicMock()
    mock_use_case = MagicMock()
    mock_spark = MagicMock()
    mock_build.return_value = (mock_use_case, mock_spark)
    mock_use_case.execute.side_effect = RuntimeError("erro crítico no pipeline")

    with patch.object(sys, "argv", ["cli.py", "--config-path", "config.yml"]):
        with pytest.raises(SystemExit) as exc_info:
            main()

    assert exc_info.value.code == 1
    mock_spark.stop.assert_called_once()


@patch(f"{_MODULE}.build_deduplication_use_case")
@patch(f"{_MODULE}.load_deduplicate_workflow_config")
def test_main_passes_config_path_from_args_to_loader(mock_loader, mock_build):
    mock_loader.return_value = MagicMock()
    mock_build.return_value = (MagicMock(), MagicMock())

    with patch.object(sys, "argv", ["cli.py", "--config-path", "/custom/path/env.yml"]):
        main()

    mock_loader.assert_called_once_with("/custom/path/env.yml")
