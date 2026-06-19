import pytest
import sys
from unittest.mock import patch, MagicMock, call

from cli import main


@patch("cli.load_yaml")
@patch("cli.build_linkage_use_case")
def test_cli_executa_linkage_com_especificacao_explicita(
    mock_build_linkage,
    mock_load_yaml,
    mock_env_yaml_content,
    mock_spec_yaml_content,
):
    mock_use_case = MagicMock()
    mock_enriched_config = MagicMock()
    mock_spark = MagicMock()
    mock_build_linkage.return_value = (mock_use_case, MagicMock(), mock_enriched_config, mock_spark)

    mock_env_local_data = mock_env_yaml_content

    def side_effect_load(path):
        if path == "env.yml":
            return mock_env_local_data
        if path == "spec.yml":
            return mock_spec_yaml_content
        return {}

    mock_load_yaml.side_effect = side_effect_load

    test_args = ["cli.py", "linkage", "--env-config", "env.yml", "--spec-config", "spec.yml"]
    with patch.object(sys, "argv", test_args):
        main()

    mock_build_linkage.assert_called_once_with(
        storage_config_data=mock_env_local_data["storage"],
        execution_config_data=mock_env_local_data["execution"],
        linkage_spec_data=mock_spec_yaml_content,
        es_config_data=mock_env_local_data["elasticsearch"],
        spark_config_data=mock_env_local_data["spark"],
    )
    mock_use_case.execute.assert_called_once()
    mock_spark.stop.assert_called_once()


@patch("cli.load_yaml")
@patch("cli.build_linkage_use_case")
def test_cli_executa_linkage_buscando_specification_path_do_env_yaml(
    mock_build_linkage,
    mock_load_yaml,
    mock_env_yaml_content,
    mock_spec_yaml_content,
):
    mock_use_case = MagicMock()
    mock_spark = MagicMock()
    mock_build_linkage.return_value = (mock_use_case, MagicMock(), MagicMock(), mock_spark)

    spec_path = "tests/configs/specifications/nascimentos/linkage/linkage_sequential.yml"
    mock_env_yaml_content["specification"]["linkage_path"] = spec_path
    mock_env_local_data = mock_env_yaml_content

    def side_effect_load(path):
        if path == "env.yml":
            return mock_env_local_data
        if path == spec_path:
            return mock_spec_yaml_content
        return {}

    mock_load_yaml.side_effect = side_effect_load

    test_args = ["cli.py", "linkage", "--env-config", "env.yml"]
    with patch.object(sys, "argv", test_args):
        main()

    mock_build_linkage.assert_called_once_with(
        storage_config_data=mock_env_local_data["storage"],
        execution_config_data=mock_env_local_data["execution"],
        linkage_spec_data=mock_spec_yaml_content,
        es_config_data=mock_env_local_data["elasticsearch"],
        spark_config_data=mock_env_local_data["spark"],
    )
    mock_spark.stop.assert_called_once()


@patch("cli.load_yaml")
@patch("cli.build_indexing_use_case")
def test_cli_executa_indexing_com_sucesso(
    mock_build_indexing,
    mock_load_yaml,
    mock_env_yaml_content,
    mock_spec_yaml_content,
):
    mock_use_case = MagicMock()
    mock_spark = MagicMock()
    mock_build_indexing.return_value = (mock_use_case, MagicMock(), mock_spark)

    def side_effect_load(path):
        if path == "env.yml":
            return mock_env_yaml_content
        if path == "spec_idx.yml":
            return mock_spec_yaml_content
        return {}

    mock_load_yaml.side_effect = side_effect_load

    test_args = ["cli.py", "indexing", "--env-config", "env.yml", "--spec-config", "spec_idx.yml"]
    with patch.object(sys, "argv", test_args):
        main()

    mock_build_indexing.assert_called_once_with(
        storage_config_data=mock_env_yaml_content["storage"],
        indexing_spec_data=mock_spec_yaml_content,
        es_config_data=mock_env_yaml_content["elasticsearch"],
        spark_config_data=mock_env_yaml_content["spark"],
    )
    mock_use_case.execute.assert_called_once()
    mock_spark.stop.assert_called_once()


def test_cli_rejeita_caso_de_uso_invalido():
    test_args = ["cli.py", "invalid_use_case", "--env-config", "env.yml"]
    with patch.object(sys, "argv", test_args):
        with pytest.raises(SystemExit):
            main()


def test_cli_obriga_env_config():
    test_args = ["cli.py", "linkage"]
    with patch.object(sys, "argv", test_args):
        with pytest.raises(SystemExit):
            main()
