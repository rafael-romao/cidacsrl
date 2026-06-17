import pytest


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
