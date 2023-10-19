from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Literal, Optional

import pytest
from pydantic import BaseModel, SecretBytes, SecretStr
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
