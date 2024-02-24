import inspect
import sys
import typing
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Annotated, Dict, Tuple, Type, Union, get_args, get_origin

from pydantic import BaseModel, ConfigDict, SecretBytes, SecretStr
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa
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

from sparkdantic.generation import GenerationSpec

if sys.version_info > (3, 10):
    from types import UnionType  # pragma: no cover
else:
    UnionType = Union  # pragma: no cover

if sys.version_info > (3, 11):
    from enum import EnumType  # pragma: no cover
else:
    EnumType = Type[Enum]  # pragma: no cover


native_spark_types = [
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
]

type_map = MappingProxyType(
    {
        int: IntegerType,
        float: DoubleType,
        str: StringType,
        SecretStr: StringType,
        bool: BooleanType,
        bytes: BinaryType,
        SecretBytes: BinaryType,
        list: ArrayType,
        dict: MapType,
        datetime: TimestampType,
        date: DateType,
        Decimal: DecimalType,
        timedelta: DayTimeIntervalType,
    }
)

MixinType = Union[Type[int], Type[str]]

GenerationSpecs = Dict[str, GenerationSpec]


class SparkModel(BaseModel):
    """Spark Model representing a Pydantic BaseModel with additional methods to convert it to a PySpark schema.

    Methods:
        model_spark_schema: Generates a PySpark schema from the model fields.
        _is_nullable: Determines if a type is nullable and returns the type without the Union.
        _get_spark_type: Returns the corresponding PySpark data type for a given Python type, considering nullability.
        _get_enum_mixin_type: Returns the mixin type of Enum.
        _type_to_spark: Converts a given Python type to a corresponding PySpark data type.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)

    @classmethod
    def model_spark_schema(cls) -> StructType:
        """Generates a PySpark schema from the model fields.

        Returns:
            StructType: The generated PySpark schema.
        """
        fields = []
        for k, v in cls.model_fields.items():
            _, t = cls._is_nullable(v.annotation)
            if cls._is_spark_model_subclass(t):
                fields.append(StructField(k, t.model_spark_schema()))  # type: ignore
            else:
                t, nullable = cls._type_to_spark(v.annotation, v.metadata)
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

    @classmethod
    def _is_spark_model_subclass(cls, value):
        """Checks if a class is a subclass of SparkModel.

        Args:
            value: The class to check.

        Returns:
            bool: True if it is a subclass, otherwise False.
        """
        try:
            return (inspect.isclass(value)) and (issubclass(value, SparkModel))
        except TypeError:
            return False

    @staticmethod
    def _get_spark_type(t: Type, nullable: bool) -> Type[DataType]:
        """Returns the corresponding PySpark data type for a given Python type, considering nullability.

        Args:
            t (Type): The type to convert to a PySpark data type.
            nullable (bool): Indicates whether the PySpark data type should be nullable.

        Returns:
            DataType: The corresponding PySpark data type.

        Raises:
            TypeError: If the type is not recognized in the type map.
        """
        spark_type = type_map.get(t)
        if spark_type is None:
            raise TypeError(f'Type {t} not recognized')

        spark_type.nullable = nullable
        return spark_type

    @classmethod
    def _get_enum_mixin_type(cls, t: EnumType) -> MixinType:
        """Returns the mixin type of `Enum`.

        Args:
            t (EnumType): The Enum to get the mixin type from.

        Returns:
            MixinType: The type mixed with the Enum.

        Raises:
            TypeError: If the mixin type is not supported (int and str are supported).
        """
        if issubclass(t, int):
            return int
        elif issubclass(t, str):
            return str
        else:
            raise TypeError(f'Enum {t} is not supported. Only int and str mixins are supported.')

    @classmethod
    def _type_to_spark(cls, t: Type, metadata) -> Tuple[DataType, bool]:
        """Converts a given Python type to a corresponding PySpark data type.

        Args:
            t (Type): The type to convert to a PySpark data type.

        Returns:
            DataType: The corresponding PySpark data type.
        """
        nullable, t = cls._is_nullable(t)
        meta = None if len(metadata) < 1 else metadata.pop()
        args = get_args(t)
        origin = get_origin(t)

        # Convert complex types
        if origin is list:
            inner_type = args[0]
            if cls._is_spark_model_subclass(inner_type):
                array_type = inner_type.model_spark_schema()
            else:
                # Check if it's an accepted Enum
                array_type, _ = cls._type_to_spark(inner_type, [])
            return ArrayType(array_type, nullable), nullable
        elif origin is dict:
            key_type, _ = cls._type_to_spark(args[0], [])
            value_type, _ = cls._type_to_spark(args[1], [])
            return MapType(key_type, value_type, nullable), nullable
        elif origin is typing.Literal:
            # PySpark doesn't have an equivalent type for Literal. To allow Literal usage with a model we check all the
            # types of values within Literal. If they are all the same, use that type as our new type.
            literal_arg_types = set(map(lambda a: type(a), args))
            if len(literal_arg_types) > 1:
                raise TypeError(
                    'Your model has a `Literal` type with multiple args of different types. Fields defined with '
                    '`Literal` must have one consistent arg type'
                )
            t = literal_arg_types.pop()
        elif origin is Annotated:
            # first arg of annotated type is the type, second is metadata that we don't do anything with (yet)
            t = args[0]

        if issubclass(t, Enum):
            t = cls._get_enum_mixin_type(t)

        if t in native_spark_types:
            spark_type = t()
        else:
            spark_type = cls._get_spark_type(t, nullable)()

        if isinstance(spark_type, DecimalType):
            if meta is not None:
                max_digits = getattr(meta, 'max_digits', 10)
                decimal_places = getattr(meta, 'decimal_places', 0)
                spark_type = DecimalType(precision=max_digits, scale=decimal_places)
        return spark_type, nullable

    @classmethod
    def generate_data(
        cls, spark: SparkSession, specs: GenerationSpecs, n_rows: int = 100
    ) -> DataFrame:
        initial_schema = StructType(
            [StructField('__sparkdantic_row_id__', IntegerType(), nullable=False)]
        )
        data = spark.createDataFrame([(i,) for i in range(n_rows)], initial_schema)

        schema = cls.model_spark_schema()
        for col in schema.fields:
            func = specs.get(col.name, None)
            generator_udf = F.udf(func, col.dataType)
            data = data.withColumn(col.name, generator_udf() if func is not None else F.lit(None))

        return data.drop('__sparkdantic_row_id__')
