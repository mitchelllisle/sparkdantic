from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Literal, Optional

import pytest
from pydantic import BaseModel, Field, SecretBytes, SecretStr
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sparkdantic.model import SparkModel


class DecimalModel(SparkModel):
    a: Decimal
    b: Decimal = Field(decimal_places=2)
    c: Decimal = Field(decimal_places=2, max_digits=5)


class RawValuesModel(SparkModel):
    a: int
    b: float
    c: str
    d: bool
    e: bytes
    f: Decimal
    y: date
    cc: datetime
    gg: timedelta
    hh: DoubleType
    ii: SecretBytes
    jj: SecretStr
    x: str = Field(alias='_x')


def test_raw_values():
    expected_schema = StructType(
        [
            StructField('a', IntegerType(), False),
            StructField('b', DoubleType(), False),
            StructField('c', StringType(), False),
            StructField('d', BooleanType(), False),
            StructField('e', BinaryType(), False),
            StructField('f', DecimalType(10, 0), False),
            StructField('y', DateType(), False),
            StructField('cc', TimestampType(), False),
            StructField('gg', DayTimeIntervalType(0, 3), False),
            StructField('hh', DoubleType(), False),
            StructField('ii', BinaryType(), False),
            StructField('jj', StringType(), False),
            StructField('_x', StringType(), False),
        ]
    )
    generated_schema = RawValuesModel.model_spark_schema()
    assert generated_schema == expected_schema


def test_literal():
    expected = StructType(
        [
            StructField('is_this_a_field', StringType(), False),
            StructField('is_this_another_field', StringType(), True),
        ]
    )

    class MyClass(BaseModel):
        is_this_a_field: Literal['yes', 'no']

    class SparkMyClass(SparkModel, MyClass):
        is_this_another_field: Optional[Literal['yes', 'no']]

    schema = SparkMyClass.model_spark_schema()
    assert schema == expected


def test_inconsistent_literal():
    class MyClass(SparkModel):
        is_this_a_field: Literal['yes', 1]

    with pytest.raises(TypeError):
        MyClass.model_spark_schema()


def test_annotated_type():
    class MyClass(SparkModel):
        optional_field: Optional[Annotated[int, Field(lt=1, gt=10)]]
        required_field: Annotated[int, Field(lt=1, gt=10)]

    schema = MyClass.model_spark_schema()
    assert schema == StructType(
        [
            StructField('optional_field', IntegerType(), True),
            StructField('required_field', IntegerType(), False),
        ]
    )


def test_decimal_types():
    expected_schema = StructType(
        [
            StructField('a', DecimalType(10, 0), False),
            StructField('b', DecimalType(10, 2), False),
            StructField('c', DecimalType(5, 2), False),
        ]
    )
    generated_schema = DecimalModel.model_spark_schema()
    assert generated_schema == expected_schema
