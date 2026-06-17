import os
import pytest

# Garante que o conector Spark-ES esteja no classpath de QUALQUER SparkContext
# criado nesta suíte — deve ser definido antes do primeiro SparkContext.init().
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--packages org.elasticsearch:elasticsearch-spark-30_2.12:9.1.8 pyspark-shell",
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
