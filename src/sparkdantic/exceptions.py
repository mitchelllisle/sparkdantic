class TypeConversionError(Exception):
    """Error converting a model field type to a PySpark type"""


class SparkdanticImportError(ImportError):
    """
    Wrapper class for ImportError to support error classes.
    """
