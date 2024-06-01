import pytest
from faker import Faker
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope='session')
def faker() -> Faker:
    return Faker()
