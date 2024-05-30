import inspect
import sys
import typing
from collections import deque
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Annotated, Dict, List, Optional, Tuple, Type, Union, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel, ConfigDict, SecretBytes, SecretStr
from pydantic.fields import ModelPrivateAttr
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
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sparkdantic.generation import GenerationSpec, MappingSpec, default_generator

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
    LongType,
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
        UUID: StringType,
    }
)

MixinType = Union[Type[int], Type[str]]

GenerationSpecs = Optional[Dict[str, GenerationSpec]]


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
    _mapped_field: ModelPrivateAttr = deque()
    _non_standard_fields: ModelPrivateAttr = {'value', 'mapping', 'mapping_source'}

    @classmethod
    def _post_mapping_process(cls, data: DataFrame) -> DataFrame:
        """Processes the DataFrame after mapping.

        Args:
            data (DataFrame): The data frame to process.

        Returns:
            DataFrame: The processed DataFrame.
        """
        for _ in range(len(cls._mapped_field.default)):
            target, mapping, source = cls._mapped_field.default.popleft()
            mapping_expr = F.create_map([F.lit(x) for x in sum(mapping.items(), ())])
            data = data.withColumn(target, mapping_expr.getItem(data[source]))
        return data

    @classmethod
    def model_spark_schema(cls) -> StructType:
        """Generates a PySpark schema from the model fields.

        Returns:
            StructType: The generated PySpark schema.
        """
        return create_spark_schema(cls)

    @classmethod
    def generate_data(
        cls, spark: SparkSession, specs: Optional[GenerationSpecs] = None, n_rows: int = 100
    ) -> DataFrame:
        # initialise a dataframe with a single column to hold the row id
        initial_schema = StructType(
            [StructField('__sparkdantic_row_id__', IntegerType(), nullable=False)]
        )
        data = spark.createDataFrame([(i,) for i in range(n_rows)], initial_schema)
        if not specs:
            specs = {}

        schema = cls.model_spark_schema()
        for col in schema.fields:
            func = specs.get(col.name, default_generator(col.dataType))
            if isinstance(func, MappingSpec):
                mapping_ = F.create_map([F.lit(x) for x in sum(func.mapping.items(), ())])
                data = data.withColumn(col.name, mapping_.getItem(data[func.mapping_source]))
            else:
                generator_udf = F.udf(func, col.dataType)
                data = data.withColumn(
                    col.name, generator_udf() if func is not None else F.lit(None)
                )

        # remove the internal row id column from the final dataframe
        return data.drop('__sparkdantic_row_id__')


def create_spark_schema(model: Type) -> StructType:
    """Generates a PySpark schema from the model fields.

    Args:
        model (Type): The pydantic model to generate the schema from.

    Returns:
        StructType: The generated PySpark schema.
    """
    fields = []
    for k, v in model.model_fields.items():
        k = getattr(v, 'alias') or k
        nullable, t = _is_nullable(v.annotation)

        if _is_supported_subclass(t):
            fields.append(StructField(k, create_spark_schema(t), nullable))  # type: ignore
        else:
            field_info_extra = getattr(v, 'json_schema_extra', None) or {}
            override = field_info_extra.get('spark_type')
            if (override not in native_spark_types) and (override is not None):
                raise TypeError(
                    f'Defining `spark_type` must be a valid `pyspark.sql.types` type. Got {override}'
                )
            if override:
                t, nullable = _type_to_spark(override, v.metadata)
            else:
                t, nullable = _type_to_spark(v.annotation, v.metadata)
            _struct_field = StructField(k, t, nullable)
            fields.append(_struct_field)
    return StructType(fields)


def _get_spark_type(t: Type, nullable: bool) -> Type[DataType]:
    """Returns the corresponding PySpark data type for a given Python type, considering nullability.

    Args:
        t (Type): The Python type to convert to a PySpark data type.
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


def _get_enum_mixin_type(t: EnumType) -> MixinType:
    """Returns the mixin type of an Enum.

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


def _type_to_spark_type_specs(t: Type) -> Tuple[DataType, Optional[str], bool]:
    """Converts a given type to its corresponding Spark data type specifications.

    Args:
        t (Type): The Python type.

    Returns:
        Tuple[DataType, Optional[str], bool]: The corresponding Spark DataType, container type, and nullability.
    """
    spark_type, nullable = _type_to_spark(t, [])
    if isinstance(spark_type, ArrayType):
        return spark_type.elementType, ArrayType.typeName(), nullable
    return spark_type, None, nullable


def _type_to_spark(t: Type, metadata) -> Tuple[DataType, bool]:
    """Converts a given Python type to a corresponding PySpark data type.

    Args:
        t (Type): The type to convert to a PySpark data type.

    Returns:
        DataType: The corresponding PySpark data type.
    """
    nullable, t = _is_nullable(t)
    meta = None if len(metadata) < 1 else metadata.pop()
    args = get_args(t)
    origin = get_origin(t)

    # Convert complex types
    if origin is list:
        inner_type = args[0]
        if _is_supported_subclass(inner_type):
            array_type = create_spark_schema(inner_type)
            inner_nullable = nullable
        else:
            # Check if it's an accepted Enum or optional SparkModel subclass
            array_type, inner_nullable = _type_to_spark(inner_type, [])
        return ArrayType(array_type, inner_nullable), nullable
    elif origin is dict:
        key_type, _ = _type_to_spark(args[0], [])
        value_type, inner_nullable = _type_to_spark(args[1], [])
        return MapType(key_type, value_type, inner_nullable), nullable
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
    elif _is_supported_subclass(t):
        return create_spark_schema(t), nullable

    if issubclass(t, Enum):
        t = _get_enum_mixin_type(t)

    if t in native_spark_types:
        t = t()
    else:
        t = _get_spark_type(t, nullable)()

    if isinstance(t, DecimalType):
        if meta is not None:
            max_digits = getattr(meta, 'max_digits', 10)
            decimal_places = getattr(meta, 'decimal_places', 0)
            t = DecimalType(precision=max_digits, scale=decimal_places)
    return t, nullable


def _is_supported_subclass(subclass: Type) -> bool:
    """Checks if a class is a subclass of SparkModel.

    Args:
        subclass: The class to check.

    Returns:
        bool: True if it is a subclass, otherwise False.
    """
    try:
        return (inspect.isclass(subclass)) and (
            issubclass(subclass, SparkModel) or issubclass(subclass, BaseModel)
        )
    except TypeError:
        return False


def _is_nullable(t: Type) -> Tuple[bool, Type]:
    """Determines if a type is nullable and returns the type without the Union.

    Args:
        t (Type): The Python type to check for nullability.

    Returns:
        Tuple[bool, Type]: A tuple containing a boolean indicating nullability and the original Python type.
    """
    if get_origin(t) in (Union, UnionType):
        type_args = get_args(t)
        if any([get_origin(arg) is None for arg in type_args]):
            t = type_args[0]
            return True, t
    return False, t
