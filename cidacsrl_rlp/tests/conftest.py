import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pytest

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

@pytest.fixture(scope="function", autouse=True)
def clean_output_folder(test_paths):
    """Garante que a pasta de output esteja vazia antes de cada teste rodar."""
    output_dir = test_paths["output_data"]
    # Cria a pasta caso ela ainda não exista fisicamente no clone local
    output_dir.mkdir(parents=True, exist_ok=True)
    
    import shutil
    for item in output_dir.iterdir():
        try:
            if item.is_file() and item.name != ".gitkeep":
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        except PermissionError:
            pass 
    yield

@pytest.fixture(scope="module", autouse=True)
def force_spark_context_teardown():
    """
    Roda automaticamente ao final de CADA arquivo de teste do projeto.
    Força a JVM do Spark a fechar de verdade, garantindo que o próximo
    arquivo consiga carregar seus próprios JARs (como o do Elasticsearch).
    """
    yield  # Deixa o arquivo de teste rodar completamente
    
    # Executado imediatamente APÓS o último teste do módulo finalizar
    try:
        from pyspark import SparkContext
        from pyspark.sql import SparkSession
        
        # 1. Captura a sessão ativa de alto nível se houver e encerra
        active_session = SparkSession.getActiveSession()
        if active_session:
            active_session.stop()
            
        # 2. Desliga o motor físico da JVM de forma síncrona
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
            
    except ImportError:
        pass

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