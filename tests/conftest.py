import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    spark_session = (
        SparkSession.builder.master('local[1]')
        .appName('sparkdantic-tests')
        .config('spark.driver.host', '127.0.0.1')
        .config('spark.driver.bindAddress', '127.0.0.1')
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()
