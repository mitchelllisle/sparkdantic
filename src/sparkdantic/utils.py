from packaging.version import Version

from .exceptions import SparkdanticImportError

MIN_PYSPARK_VERSION = '3.3.0'
"""Inclusive minimum version of PySpark required"""
MAX_PYSPARK_VERSION = '4.0.0'
"""Exclusive maximum version of PySpark supported"""

try:
    import pyspark
except ImportError as raised_error:
    have_pyspark = False
    pyspark_import_error = raised_error
else:
    have_pyspark = True
    pyspark_import_error = None  # type: ignore


def require_pyspark() -> None:
    """
    Raise SparkdanticImportError if PySpark is not installed
    """
    if not have_pyspark:
        raise SparkdanticImportError(
            'Pyspark is not installed. Install pyspark using `pip install sparkdantic[pyspark]`'
        ) from pyspark_import_error


def require_pyspark_version_in_range() -> None:
    """
    Raise SparkdanticImportError if an unsupported version of PySpark is installed
    """

    if not (
        Version(MIN_PYSPARK_VERSION) <= Version(pyspark.__version__) < Version(MAX_PYSPARK_VERSION)
    ):
        raise SparkdanticImportError(
            f'Pyspark version >={MIN_PYSPARK_VERSION},<{MAX_PYSPARK_VERSION} is required, '
            f'but found {pyspark.__version__}'
        )
