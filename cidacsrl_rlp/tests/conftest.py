import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pytest

@pytest.fixture(scope="session")
def test_paths():
    """Centraliza as rotas absolutas do ambiente de teste para evitar caminhos quebrados."""
    # Como este ficheiro está em tests/configs/conftest.py, 
    # o .parent aponta para tests/configs e o .parent.parent aponta para tests/
    tests_root = Path(__file__).parent.parent.resolve()
    
    return {
        "configs": tests_root / "configs",
        "input_data": tests_root / "data" / "input",
        "output_data": tests_root / "data" / "output",
        "env_yaml": tests_root / "configs" / "env_local.yml",
        "indexing_yaml": tests_root / "configs" / "specifications" / "nascimentos" / "indexing" / "indexing_nascimentos.yml"
    }

@pytest.fixture(scope="function", autouse=True)
def clean_output_folder(test_paths):
    """Garante que a pasta de output esteja vazia antes de cada teste rodar."""
    output_dir = test_paths["output_data"]
    # Cria a pasta caso ela ainda não exista fisicamente no clone local
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for file in output_dir.iterdir():
        if file.is_file() and file.name != ".gitkeep":
            file.unlink()
        elif file.is_dir():
            import shutil
            shutil.rmtree(file)
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