import dbldatagen as dg
import pytest
from faker import Faker


@pytest.fixture(scope='module')
def spark():
    spark_session = dg.SparkSingleton.getLocalInstance('unit tests')
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope='module')
def faker() -> Faker:
    return Faker()
