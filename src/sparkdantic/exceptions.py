class TypeConversionError(Exception):
    """Error converting a model field type to a PySpark type"""


class SparkdanticImportError(ImportError):
    """Error importing PySpark. Raised when PySpark is not installed or the installed version is not supported."""
