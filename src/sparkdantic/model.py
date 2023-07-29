import inspect
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Tuple, Type, Union, get_args, get_origin

from pydantic import BaseModel, ConfigDict
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if sys.version_info > (3, 10):
    from types import UnionType
else:
    UnionType = Union  # pragma: no cover

type_map = MappingProxyType(
    {
        int: IntegerType,
        float: DoubleType,
        str: StringType,
        bool: BooleanType,
        bytes: BinaryType,
        list: ArrayType,
        dict: MapType,
        datetime: TimestampType,
        date: DateType,
        Decimal: DecimalType,
        timedelta: DayTimeIntervalType,
    }
)


class SparkModel(BaseModel):
    """Spark Model representing a Pydantic BaseModel with additional methods to convert it to a PySpark schema.

    Methods:
        model_spark_schema: Generates a PySpark schema from the model fields.
        _is_nullable: Determines if a type is nullable and returns the type without the Union.
        _get_spark_type: Returns the corresponding PySpark data type for a given Python type, considering nullability.
        _type_to_spark: Converts a given Python type to a corresponding PySpark data type.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def model_spark_schema(cls) -> StructType:
        """Generates a PySpark schema from the model fields.

        Returns:
            StructType: The generated PySpark schema.
        """
        fields = []
        for k, v in cls.model_fields.items():
            if (inspect.isclass(v.annotation)) and (issubclass(v.annotation, SparkModel)):
                _struct_field = StructField(k, v.annotation.model_spark_schema(), True)
                fields.append(StructField(k, v.annotation.model_spark_schema()))
            else:
                t, nullable = cls._type_to_spark(v.annotation)
                _struct_field = StructField(k, t, nullable)
                fields.append(_struct_field)
        return StructType(fields)

    @staticmethod
    def _is_nullable(t: Type) -> Tuple[bool, Type]:
        """Determines if a type is nullable and returns the type without the Union.

        Args:
            t (Type): The type to check for nullability.

        Returns:
            Tuple[bool, Type]: A tuple containing a boolean indicating nullability and the original type.
        """
        if get_origin(t) in (Union, UnionType):
            type_args = get_args(t)
            if any([get_origin(arg) is None for arg in type_args]):
                t = type_args[0]
                return True, t
        return False, t

    @staticmethod
    def _get_spark_type(t: Type, nullable: bool) -> DataType:
        """Returns the corresponding PySpark data type for a given Python type, considering nullability.

        Args:
            t (Type): The type to convert to a PySpark data type.
            nullable (bool): Indicates whether the PySpark data type should be nullable.

        Returns:
            DataType: The corresponding PySpark data type.
        """
        spark_type = type_map[t]
        spark_type.nullable = nullable
        return spark_type()

    @classmethod
    def _type_to_spark(cls, t: Type) -> Tuple[DataType, bool]:
        """Converts a given Python type to a corresponding PySpark data type.

        Args:
            t (Type): The type to convert to a PySpark data type.

        Returns:
            DataType: The corresponding PySpark data type.

        Raises:
            TypeError: If the type is not recognized in the type map.
        """
        nullable, t = cls._is_nullable(t)

        args = get_args(t)
        origin = get_origin(t)

        if origin is list:
            array_type = cls._get_spark_type(args[0], nullable)
            return ArrayType(array_type, nullable), nullable
        elif origin is dict:
            key_type, _ = cls._type_to_spark(args[0])
            value_type, _ = cls._type_to_spark(args[1])
            return MapType(key_type, value_type, nullable), nullable

        try:
            spark_type = type_map[t]
            spark_type.nullable = nullable
            return spark_type(), nullable
        except KeyError:
            raise TypeError(f'Type {t} not recognized')
