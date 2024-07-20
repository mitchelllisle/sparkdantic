import pytest
from faker import Faker


@pytest.fixture(scope='session')
def spark():
    spark_session = dg.SparkSingleton.getLocalInstance('unit tests')
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope='session')
def faker() -> Faker:
    return Faker()
