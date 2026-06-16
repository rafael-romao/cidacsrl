# cidacsrl_rlp/tests/unit/infra/adapters/inbound/test_cli.py

import pytest
import sys
from unittest.mock import patch, MagicMock

from cidacsrl.cli import main

@pytest.fixture
def mock_env_yaml_content() -> dict:
    """Mimetiza a estrutura unificada e limpa do novo env_local.yml."""
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
    """Mimetiza as regras de negócio abstratas de um arquivo de especificação."""
    return {"source_table": "tabela_origem", "target_es_index": "indice_destino"}


@patch("cidacsrl_rlp.cli.load_yaml")
@patch("cidacsrl_rlp.cli.bootstrap_sequential_linkage")
def test_cli_executa_linkage_com_especificacao_explicita(
    mock_bootstrap_linkage,
    mock_load_yaml,
    mock_env_yaml_content,
    mock_spec_yaml_content
):
    """Garante que o CLI fatiará o env.yml e chamará o bootstrapper de linkage com os argumentos da CLI."""
    # Configura o comportamento do leitor de YAML do CLI
    def side_effect_load(path):
        if path == "env.yml":
            return mock_env_local_data
        if path == "spec.yml":
            return mock_spec_yaml_content
        return {}
    
    mock_env_local_data = mock_env_yaml_content
    mock_load_yaml.side_effect = side_effect_load

    # Simula a chamada de linha de comando: python -m cidacsrl_rlp.cli linkage --env-config env.yml --spec-config spec.yml
    test_args = ["cli.py", "linkage", "--env-config", "env.yml", "--spec-config", "spec.yml"]
    
    with patch.object(sys, "argv", test_args):
        main()

    # Verifica se os sub-blocos foram fatiados e repassados sem novas leituras desnecessárias de disco
    mock_bootstrap_linkage.assert_called_once_with(
        storage_config_data=mock_env_local_data["storage"],
        execution_config_data=mock_env_local_data["execution"],
        linkage_spec_data=mock_spec_yaml_content,
        es_config_data=mock_env_local_data["elasticsearch"],
        spark_config_data=mock_env_local_data["spark"]
    )


@patch("cidacsrl_rlp.cli.load_yaml")
@patch("cidacsrl_rlp.cli.bootstrap_sequential_linkage")
def test_cli_executa_linkage_buscando_specification_path_do_env_yaml(
    mock_bootstrap_linkage,
    mock_load_yaml,
    mock_env_yaml_content,
    mock_spec_yaml_content
):
    """Garante que se --spec-config for omitido, o CLI usará o linkage_path mapeado no bloco 'specification'."""
    def side_effect_load(path):
        if path == "env.yml":
            return mock_env_local_data
        if path == "tests/configs/specifications/nascimentos/linkage/linkage_sequential.yml":
            return mock_spec_yaml_content
        return {}
    
    mock_env_local_data = mock_env_yaml_content
    
    mock_env_local_data["specification"]["linkage_path"] = "tests/configs/specifications/nascimentos/linkage/linkage_sequential.yml"
    mock_load_yaml.side_effect = side_effect_load

   
    test_args = ["cli.py", "linkage", "--env-config", "env.yml"]
    
    with patch.object(sys, "argv", test_args):
        main()

   
    mock_bootstrap_linkage.assert_called_once_with(
        storage_config_data=mock_env_local_data["storage"],
        execution_config_data=mock_env_local_data["execution"],
        linkage_spec_data=mock_spec_yaml_content,
        es_config_data=mock_env_local_data["elasticsearch"],
        spark_config_data=mock_env_local_data["spark"]
    )


@patch("cidacsrl_rlp.cli.load_yaml")
@patch("cidacsrl_rlp.cli.bootstrap_elasticsearch_indexing")
def test_cli_executa_indexing_com_sucesso(
    mock_bootstrap_indexing,
    mock_load_yaml,
    mock_env_yaml_content,
    mock_spec_yaml_content
):
    """Garante que o caso de uso de indexação dispara o bootstrapper correto."""
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

    mock_bootstrap_indexing.assert_called_once_with(
        storage_config_data=mock_env_yaml_content["storage"],
        indexing_spec_data=mock_spec_yaml_content,
        es_config_data=mock_env_yaml_content["elasticsearch"],
        spark_config_data=mock_env_yaml_content["spark"]
    )


def test_cli_rejeita_caso_de_uso_invalido():
    """Valida que o argparse barra a execução imediatamente se o caso de uso não constar no choices."""
    test_args = ["cli.py", "invalid_use_case", "--env-config", "env.yml"]
    
    with patch.object(sys, "argv", test_args):
        with pytest.raises(SystemExit): 
            main()


def test_cli_obriga_env_config():
    """Garante que o parâmetro --env-config é estritamente obrigatório."""
    test_args = ["cli.py", "linkage"]
    
    with patch.object(sys, "argv", test_args):
        with pytest.raises(SystemExit):
            main()