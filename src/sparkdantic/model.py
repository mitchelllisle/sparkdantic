from pydantic import BaseModel, ConfigDict
from typing import Union, Type, Tuple, get_origin, get_args
from types import MappingProxyType
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    StringType,
    BinaryType,
    BooleanType,
    ArrayType,
    MapType,
    TimestampType,
    DecimalType,
    DayTimeIntervalType,
    DateType,
    StructType,
    StructField,
    DataType
)
from datetime import datetime, date, timedelta
from decimal import Decimal

type_map = MappingProxyType({
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
})


class SparkModel(BaseModel):
    """Spark Model representing a Pydantic BaseModel with additional methods to convert it to a PySpark schema.

    Methods:
        spark_schema: Generates a PySpark schema from the model fields.
        _is_nullable: Determines if a type is nullable and returns the type without the Union.
        _get_spark_type: Returns the corresponding PySpark data type for a given Python type, considering nullability.
        _type_to_spark: Converts a given Python type to a corresponding PySpark data type.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def spark_schema(self) -> StructType:
        """Generates a PySpark schema from the model fields.

        Returns:
            StructType: The generated PySpark schema.
        """
        return StructType([StructField(k, self._type_to_spark(v.annotation)) for k, v in self.model_fields.items()])

    @staticmethod
    def _is_nullable(t: Type) -> Tuple[bool, Type]:
        """Determines if a type is nullable and returns the type without the Union.

        Args:
            t (Type): The type to check for nullability.

        Returns:
            Tuple[bool, Type]: A tuple containing a boolean indicating nullability and the original type.
        """
        if get_origin(t) is Union:
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
        spark_type.nullable = True if nullable else False
        return spark_type()

    def _type_to_spark(self, t: Type) -> DataType:
        """Converts a given Python type to a corresponding PySpark data type.

        Args:
            t (Type): The type to convert to a PySpark data type.

        Returns:
            DataType: The corresponding PySpark data type.

        Raises:
            TypeError: If the type is not recognized in the type map.
        """
        nullable, t = self._is_nullable(t)

        args = get_args(t)
        origin = get_origin(t)

        if origin is list:
            array_type = self._get_spark_type(args[0], nullable)
            return ArrayType(array_type, nullable)
        elif origin is dict:
            key_type = self._type_to_spark(args[0])
            value_type = self._type_to_spark(args[1])
            return MapType(key_type, value_type, nullable)

        try:
            spark_type = type_map[t]
            spark_type.nullable = True if nullable else False
            return spark_type()
        except KeyError:
            raise TypeError(f"Type {t} not recognized")
