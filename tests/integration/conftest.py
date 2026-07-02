import os
import pytest

# Garante que o conector Spark-ES esteja no classpath de QUALQUER SparkContext
# criado nesta suíte — deve ser definido antes do primeiro SparkContext.init().
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--packages org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8 pyspark-shell",
)


@pytest.fixture(scope="session", autouse=True)
def require_elasticsearch():
    """Aborta a sessão se o Elasticsearch não estiver acessível."""
    from elasticsearch import Elasticsearch

    url = os.environ.get("CIDACSRL_ES_URL", "http://localhost:9200")
    client = Elasticsearch(url, request_timeout=5)
    if not client.ping():
        pytest.exit(
            f"\n❌ Elasticsearch não está respondendo em {url}.\n"
            "   Execute 'make up' para subir o ambiente antes de rodar os testes de integração.\n",
            returncode=2,
        )


@pytest.fixture(scope="module", autouse=True)
def force_spark_context_teardown():
    yield
    try:
        from pyspark import SparkContext
        from pyspark.sql import SparkSession

        active_session = SparkSession.getActiveSession()
        if active_session:
            active_session.stop()

        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()

    except ImportError:
        pass
