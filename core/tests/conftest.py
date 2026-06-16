import os
from pathlib import Path
import pytest
from core.infra.configs.logging_config import configure_logging

configure_logging()

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