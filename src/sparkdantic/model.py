import inspect
import sys
import typing
from collections import deque
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Annotated, Dict, List, Optional, Tuple, Type, Union, get_args, get_origin

import dbldatagen as dg
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

ColumnSpecs = Optional[Dict[str, ColumnGenerationSpec]]


class SparkModel(BaseModel):
    """Spark Model representing a Pydantic BaseModel with additional methods to convert it to a PySpark schema.

    Methods:
        model_spark_schema: Generates a PySpark schema from the model fields.
        _is_nullable: Determines if a type is nullable and returns the type without the Union.
        _get_spark_type: Returns the corresponding PySpark data type for a given Python type, considering nullability.
        _get_enum_mixin_type: Returns the mixin type of an Enum.
        _type_to_spark: Converts a given Python type to a corresponding PySpark data type.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)
    _mapped_field: ModelPrivateAttr = deque()
    _non_standard_fields: ModelPrivateAttr = {'value', 'mapping', 'mapping_source'}

    @classmethod
    def _type_to_spark_type_specs(cls, t: Type) -> Tuple[DataType, Optional[str], bool]:
        """Converts a given type to its corresponding Spark data type specifications.

        Args:
            t (Type): The Python type.

        Returns:
            Tuple[DataType, Optional[str], bool]: The corresponding Spark DataType, container type, and nullability.
        """
        spark_type, nullable = cls._type_to_spark(t)
        if isinstance(spark_type, ArrayType):
            return spark_type.elementType, ArrayType.typeName(), nullable
        return spark_type, None, nullable

    @classmethod
    def _spec_weights_to_row_count(
        cls, generator: dg.DataGenerator, weights: List[float]
    ) -> List[int]:
        """Converts weights to row count.

        Args:
            generator (dg.DataGenerator): The data generator.
            weights (List[float]): The list of weights.

        Returns:
            List[int]: List of row counts.
        """
        return [int(generator.rowCount * w) for w in weights]

    @classmethod
    def _add_column_specs(
        cls,
        generator: dg.DataGenerator,
        spec: ColumnGenerationSpec,
        name: str,
        field: Field,
    ):
        """Adds column specifications to the DataGenerator.

        Args:
            generator (dg.DataGenerator): The data generator.
            spec (ColumnGenerationSpec): The column generation specifications.
            name (str): The column name.
            field (Field): The Pydantic field.
        """
        t, container, nullable = cls._type_to_spark_type_specs(field.annotation)
        if spec:
            if spec.weights:
                spec.weights = cls._spec_weights_to_row_count(generator, spec.weights)  # type: ignore

            if spec.value:
                spec.values = [spec.value]

            if spec.mapping:
                if spec.mapping_source is None:
                    raise ValueError(
                        'You have specified a mapping but not mapping_source. '
                        'You must pass in a valid column name to map values to.'
                    )
                cls._mapped_field.default.append((name, spec.mapping, spec.mapping_source))

            generator.withColumn(
                name,
                colType=t,
                nullable=nullable,
                structType=container,
                **spec.model_dump(
                    by_alias=True,
                    exclude_none=True,
                    exclude=cls._non_standard_fields.default,
                ),
            )
        else:
            generator.withColumn(name, colType=t, nullable=nullable, structType=container)

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
        generator = dg.DataGenerator(spark, rows=n_rows, **kwargs)
        for name, field in cls.model_fields.items():
            spec = specs.get(name)

            if (
                spec is None
                and inspect.isclass(field.annotation)
                and issubclass(field.annotation, Enum)
            ):
                # For Enums, default to using all values specified in the Enum
                spec = ColumnGenerationSpec(values=[item.value for item in field.annotation])

            cls._add_column_specs(generator, spec, name, field)  # type: ignore
        generated = generator.build()
        return cls._post_mapping_process(generated)

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

    @classmethod
    def _is_spark_model_subclass(cls, value):
        """Checks if a class is a subclass of SparkModel.

        Args:
            value: The class to check.

        Returns:
            bool: True if it is a subclass, otherwise False.
        """
        return (inspect.isclass(value)) and (issubclass(value, SparkModel))

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

    @classmethod
    def _type_to_spark(cls, t: Type) -> Tuple[DataType, bool]:
        """Converts a given Python type to a corresponding PySpark data type.

        Args:
            t (Type): The type to convert to a PySpark data type.

        Returns:
            DataType: The corresponding PySpark data type.
        """
        nullable, t = cls._is_nullable(t)

        args = get_args(t)
        origin = get_origin(t)

        # Convert complex types
        if origin is list:
            inner_type = args[0]
            if cls._is_spark_model_subclass(inner_type):
                array_type = inner_type.model_spark_schema()
            else:
                # Check if it's an accepted Enum
                array_type, _ = cls._type_to_spark(inner_type)
            return ArrayType(array_type, nullable), nullable
        elif origin is dict:
            key_type, _ = cls._type_to_spark(args[0])
            value_type, _ = cls._type_to_spark(args[1])
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
            spark_type = t
            spark_type.nullable = nullable
        else:
            spark_type = cls._get_spark_type(t, nullable)
        return spark_type(), nullable
