import sys

import pytest
from pyspark.sql import SparkSession

# Increase recursion limit for Python 3.14+ compatibility with PySpark
# Python 3.14 reduced the default C stack size, causing pickling issues in PySpark
if sys.version_info >= (3, 14):
    sys.setrecursionlimit(3000)


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()
