import pytest
from unittest.mock import MagicMock, call, patch

from core.infra.spark.spark_factory import spark_session_context

pytestmark = pytest.mark.unit

_MODULE = "core.infra.spark.spark_factory"


@patch(f"{_MODULE}.create_spark_session")
def test_context_manager_yields_spark_session(mock_create):
    mock_spark = MagicMock()
    mock_create.return_value = mock_spark

    with spark_session_context(app_name="test-app") as spark:
        assert spark is mock_spark


@patch(f"{_MODULE}.create_spark_session")
def test_context_manager_passes_args_to_create(mock_create):
    mock_create.return_value = MagicMock()
    config = {"spark.master": "local[1]"}

    with spark_session_context(app_name="my-app", spark_config=config, checkpoint_dir="/tmp/ckpt"):
        pass

    mock_create.assert_called_once_with(
        app_name="my-app",
        spark_config=config,
        checkpoint_dir="/tmp/ckpt",
    )


@patch(f"{_MODULE}.create_spark_session")
def test_context_manager_stops_spark_on_success(mock_create):
    mock_spark = MagicMock()
    mock_create.return_value = mock_spark

    with spark_session_context(app_name="test-app"):
        pass

    mock_spark.stop.assert_called_once()


@patch(f"{_MODULE}.create_spark_session")
def test_context_manager_stops_spark_on_exception(mock_create):
    mock_spark = MagicMock()
    mock_create.return_value = mock_spark

    with pytest.raises(RuntimeError):
        with spark_session_context(app_name="test-app"):
            raise RuntimeError("falha simulada")

    mock_spark.stop.assert_called_once()


@patch(f"{_MODULE}.create_spark_session")
def test_context_manager_reraises_exception(mock_create):
    mock_create.return_value = MagicMock()
    original_error = ValueError("erro original")

    with pytest.raises(ValueError) as exc_info:
        with spark_session_context(app_name="test-app"):
            raise original_error

    assert exc_info.value is original_error


@patch(f"{_MODULE}.create_spark_session")
def test_context_manager_logs_error_on_exception(mock_create, caplog):
    import logging
    mock_create.return_value = MagicMock()

    with pytest.raises(RuntimeError):
        with caplog.at_level(logging.ERROR, logger="Factory: SparkSessionFactory"):
            with spark_session_context(app_name="meu-app"):
                raise RuntimeError("boom")

    assert "meu-app" in caplog.text
    assert "boom" in caplog.text
