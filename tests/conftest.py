import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='module')
def spark():
    spark_session = SparkSession.builder.appName('pytest').getOrCreate()
    yield spark_session
    spark_session.stop()
