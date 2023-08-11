from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

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


class OptionalValuesModel(SparkModel):
    g: Optional[int]
    h: Optional[float]
    i: Optional[str]
    j: Optional[bool]
    k: Optional[bytes]
    l: Optional[Decimal]
    z: Optional[date]
    dd: Optional[datetime]
    hh: Optional[timedelta]


def test_optional_values():
    expected_schema = StructType(
        [
            StructField('g', IntegerType(), True),
            StructField('h', DoubleType(), True),
            StructField('i', StringType(), True),
            StructField('j', BooleanType(), True),
            StructField('k', BinaryType(), True),
            StructField('l', DecimalType(10, 0), True),
            StructField('z', DateType(), True),
            StructField('dd', TimestampType(), True),
            StructField('hh', DayTimeIntervalType(0, 3), True),
        ]
    )
    generated_schema = OptionalValuesModel.model_spark_schema()
    assert generated_schema == expected_schema
