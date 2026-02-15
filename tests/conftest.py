import os
import sys

import pytest
from pyspark.sql import SparkSession

# Python 3.14 compatibility: Increase stack size for PySpark pickling operations
# Python 3.14 reduced the default C stack size, causing stack overflow during serialization
if sys.version_info >= (3, 14):
    try:
        import resource
        # Get current stack size limit
        soft, hard = resource.getrlimit(resource.RLIMIT_STACK)
        # Increase stack size to 32MB if possible
        new_stack_size = 32 * 1024 * 1024  # 32 MB
        if hard == resource.RLIM_INFINITY or new_stack_size <= hard:
            resource.setrlimit(resource.RLIMIT_STACK, (new_stack_size, hard))
    except (ImportError, ValueError, OSError):
        # If we can't set the limit, at least try to increase Python recursion limit
        sys.setrecursionlimit(5000)


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()
