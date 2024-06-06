import inspect
import sys
import typing
from collections import deque
from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Annotated, Dict, List, Optional, Tuple, Type, Union, get_args, get_origin
from uuid import UUID

import dbldatagen as dg
from annotated_types import BaseMetadata
from pydantic import BaseModel, ConfigDict, Field, SecretBytes, SecretStr
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

from sparkdantic.generation import ColumnGenerationSpec

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

ColumnSpecs = Optional[Dict[str, ColumnGenerationSpec]]


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
    def model_spark_schema(cls, safe_casting: bool = False) -> StructType:
        """Generates a PySpark schema from the model fields.
        Args:
            safe_casting (bool): Indicates whether to use safe casting for integer types.

        Returns:
            StructType: The generated PySpark schema.
        """
        return create_spark_schema(cls, safe_casting)

    @classmethod
    def generate_data(
        cls,
        spark: SparkSession,
        n_rows: int = 100,
        specs: Optional[ColumnSpecs] = None,
        **kwargs,
    ) -> DataFrame:
        """Generates PySpark DataFrame based on the schema and the column specs.

        Args:
            spark (SparkSession): The Spark session.
            n_rows (int, optional): Number of rows. Defaults to 100.
            specs (Optional[ColumnSpecs]): Column specifications. Defaults to None.

        Returns:
            DataFrame: The generated PySpark DataFrame.
        """
        specs = {} if not specs else specs
        generator = dg.DataGenerator(spark, seedColumnName='_seed_id', rows=n_rows, **kwargs)
        for name, field in cls.model_fields.items():
            spec = specs.get(name)
            name = getattr(field, 'alias') or name

            if (
                spec is None
                and inspect.isclass(field.annotation)
                and issubclass(field.annotation, Enum)
            ):
                # For Enums, default to using all values specified in the Enum
                spec = ColumnGenerationSpec(values=[item.value for item in field.annotation])

            _add_column_specs(cls, generator, spec, name, field)  # type: ignore
        generated = generator.build()
        return cls._post_mapping_process(generated)


def create_spark_schema(model: Type, safe_casting: bool = False) -> StructType:
    """Generates a PySpark schema from the model fields.

    Args:
        model (Type): The pydantic model to generate the schema from.
        safe_casting (bool): Indicates whether to use safe casting for integer types.

    Returns:
        StructType: The generated PySpark schema.
    """
    fields = []
    for k, v in model.model_fields.items():
        k = getattr(v, 'alias') or k
        nullable, t = _is_nullable(v.annotation)

        if _is_supported_subclass(t):
            fields.append(StructField(k, create_spark_schema(t, safe_casting), nullable))  # type: ignore
        else:
            field_info_extra = getattr(v, 'json_schema_extra', None) or {}
            override = field_info_extra.get('spark_type')

            if override or t in native_spark_types:
                t = _get_native_spark_type(override or t, v.metadata)
            else:
                t, nullable = _type_to_spark(v.annotation, v.metadata, safe_casting)
            _struct_field = StructField(k, t, nullable)
            fields.append(_struct_field)
    return StructType(fields)


def _get_native_spark_type(t: Type[DataType], meta: BaseMetadata) -> DataType:
    """Returns the instantiated type for a given PySpark type.

    Args:
        t (Type[DataType]): The PySpark data type.
        meta (BaseMetadata): The metadata.
    """
    if t not in native_spark_types:
        raise TypeError(f'Defining `spark_type` must be a valid `pyspark.sql.types` type. Got {t}')
    if isinstance(t, DecimalType):
        return _set_decimal_precision_and_scale(t, meta)

    return t()


def _add_column_specs(
    model: Type[SparkModel],
    generator: dg.DataGenerator,
    spec: ColumnGenerationSpec,
    name: str,
    field: Field,
):
    """Adds column specifications to the DataGenerator.

    Args:
        model (Type[SparkModel]): The Spark Model.
        generator (dg.DataGenerator): The data generator.
        spec (ColumnGenerationSpec): The column generation specifications.
        name (str): The column name.
        field (Field): The Pydantic field.
    """
    t, container, nullable = _type_to_spark_type_specs(field.annotation)
    if spec:
        if spec.weights:
            spec.weights = _spec_weights_to_row_count(generator, spec.weights)  # type: ignore

        if spec.value:
            spec.values = [spec.value]

        if spec.mapping:
            if spec.mapping_source is None:
                raise ValueError(
                    'You have specified a mapping but not mapping_source. '
                    'You must pass in a valid column name to map values to.'
                )
            model._mapped_field.default.append((name, spec.mapping, spec.mapping_source))

        generator.withColumn(
            name,
            colType=t,
            nullable=nullable,
            structType=container,
            **spec.model_dump(
                by_alias=True,
                exclude_none=True,
                exclude=model._non_standard_fields.default,
            ),
        )
    else:
        generator.withColumn(name, colType=t, nullable=nullable, structType=container)


def _get_spark_type(t: Type, nullable: bool, safe_casting: bool = False) -> Type[DataType]:
    """Returns the corresponding PySpark data type for a given Python type, considering nullability.

    Args:
        t (Type): The Python type to convert to a PySpark data type.
        nullable (bool): Indicates whether the PySpark data type should be nullable.
        safe_casting (bool): Indicates whether to use safe casting for integer types.

    Returns:
        DataType: The corresponding PySpark data type.

    Raises:
        TypeError: If the type is not recognized in the type map.
    """
    spark_type = type_map.get(t)
    if spark_type is None:
        raise TypeError(f'Type {t} not recognized')
    if safe_casting is True and spark_type.typeName() == 'integer':
        spark_type = LongType
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


def _type_to_spark(t: Type, metadata, safe_casting: bool = False) -> Tuple[DataType, bool]:
    """Converts a given Python type to a corresponding PySpark data type.

    Args:
        t (Type): The type to convert to a PySpark data type.

    Returns:
        DataType: The corresponding PySpark data type.
    """
    nullable, t = _is_nullable(t)
    meta = None if len(metadata) < 1 else deepcopy(metadata).pop()
    args = get_args(t)
    origin = get_origin(t)

    # Convert complex types
    if origin is list:
        inner_type = args[0]
        if _is_supported_subclass(inner_type):
            array_type = create_spark_schema(inner_type, safe_casting)
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
        return create_spark_schema(t, safe_casting), nullable

    if issubclass(t, Enum):
        t = _get_enum_mixin_type(t)

    if t in native_spark_types:
        t = _get_native_spark_type(t, meta)
    else:
        t = _get_spark_type(t, nullable, safe_casting)()

    if isinstance(t, DecimalType):
        t = _set_decimal_precision_and_scale(t, meta)

    return t, nullable


def _set_decimal_precision_and_scale(t: DecimalType, meta: BaseMetadata) -> DecimalType:
    """Sets the precision and scale of a DecimalType if available in the metadata. Defaults to 10 and 0.

    Args:
        t (DecimalType): The DecimalType.
        meta (BaseMetadata): The metadata.
    """
    if meta is None:
        return t
    max_digits = getattr(meta, 'max_digits', 10)
    decimal_places = getattr(meta, 'decimal_places', 0)
    return DecimalType(precision=max_digits, scale=decimal_places)


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


def _spec_weights_to_row_count(generator: dg.DataGenerator, weights: List[float]) -> List[int]:
    """Converts weights to row count.

    Args:
        generator (dg.DataGenerator): The data generator.
        weights (List[float]): The list of weights.

    Returns:
        List[int]: List of row counts.
    """
    return [int(generator.rowCount * w) for w in weights]
