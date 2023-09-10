import inspect
import sys
from collections import deque
from datetime import date, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Dict, List, Optional, Tuple, Type, Union, get_args, get_origin

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
    from types import UnionType
else:
    UnionType = Union  # pragma: no cover

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

ColumnSpecs = Optional[Dict[str, ColumnGenerationSpec]]


class SparkModel(BaseModel):
    """Spark Model representing a Pydantic BaseModel with additional methods to convert it to a PySpark schema.

    Methods:
        model_spark_schema: Generates a PySpark schema from the model fields.
        _is_nullable: Determines if a type is nullable and returns the type without the Union.
        _get_spark_type: Returns the corresponding PySpark data type for a given Python type, considering nullability.
        _type_to_spark: Converts a given Python type to a corresponding PySpark data type.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    _mapped_field: ModelPrivateAttr = deque()
    _non_standard_fields: ModelPrivateAttr = {'value', 'mapping', 'mapping_source'}

    @classmethod
    def _type_to_spark_type_specs(cls, t: Type) -> Tuple[DataType, Optional[str], bool]:
        spark_type, nullable = cls._type_to_spark(t)
        if isinstance(spark_type, ArrayType):
            return spark_type.elementType, ArrayType.typeName(), nullable
        return spark_type, None, nullable

    @classmethod
    def _spec_weights_to_row_count(
        cls, generator: dg.DataGenerator, weights: List[float]
    ) -> List[int]:
        return [int(generator.rowCount * w) for w in weights]

    @classmethod
    def _add_column_specs(
        cls, generator: dg.DataGenerator, spec: ColumnGenerationSpec, name: str, field: Field
    ):
        t, container, nullable = cls._type_to_spark_type_specs(field.annotation)
        if spec:
            if spec.weights:
                spec.weights = cls._spec_weights_to_row_count(generator, spec.weights)  # type: ignore

            if spec.value:
                spec.values = [spec.value]

            if spec.mapping:
                if spec.mapping_source is None:
                    raise ValueError(
                        'You have specified a mapping but not mapping_source. You must pass ina valid column name to'
                        ' map values to.'
                    )
                cls._mapped_field.default.append((name, spec.mapping, spec.mapping_source))

            generator.withColumn(
                name,
                colType=t,
                nullable=nullable,
                structType=container,
                **spec.model_dump(
                    by_alias=True, exclude_none=True, exclude=cls._non_standard_fields.default
                ),
            )
        else:
            generator.withColumn(name, colType=t, nullable=nullable, structType=container)

    @classmethod
    def _post_mapping_process(cls, data: DataFrame) -> DataFrame:
        for _ in range(len(cls._mapped_field.default)):
            target, mapping, source = cls._mapped_field.default.popleft()
            mapping_expr = F.create_map([F.lit(x) for x in sum(mapping.items(), ())])
            data = data.withColumn(target, mapping_expr.getItem(data[source]))
        return data

    @classmethod
    def generate_data(
        cls, spark: SparkSession, n_rows: int = 100, specs: Optional[ColumnSpecs] = None, **kwargs
    ) -> DataFrame:
        specs = {} if not specs else specs
        generator = dg.DataGenerator(spark, rows=n_rows, **kwargs)
        for name, field in cls.model_fields.items():
            spec = specs.get(name)
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
        return (inspect.isclass(value)) and (issubclass(value, SparkModel))

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
            if cls._is_spark_model_subclass(args[0]):
                array_type = args[0].model_spark_schema()
            else:
                array_type = cls._get_spark_type(args[0], nullable)
            return ArrayType(array_type, nullable), nullable
        elif origin is dict:
            key_type, _ = cls._type_to_spark(args[0])
            value_type, _ = cls._type_to_spark(args[1])
            return MapType(key_type, value_type, nullable), nullable

        try:
            if t in native_spark_types:
                spark_type = t
            else:
                spark_type = type_map[t]
            spark_type.nullable = nullable
            return spark_type(), nullable
        except KeyError:
            raise TypeError(f'Type {t} not recognized')
