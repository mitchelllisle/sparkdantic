from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import IntEnum
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

from sparkdantic import SparkModel


def test_optional_model_fields():
    class IntTestEnum(IntEnum):
        X = 1
        Y = 2

    class OptionalsModel(SparkModel):
        g: Optional[int]
        h: Optional[float]
        i: Optional[str]
        j: Optional[bool]
        k: Optional[bytes]
        y: Optional[Decimal]
        z: Optional[date]
        dd: Optional[datetime]
        hh: Optional[timedelta]
        ii: Optional[IntTestEnum]

    expected_schema = StructType(
        [
            StructField('g', IntegerType(), True),
            StructField('h', DoubleType(), True),
            StructField('i', StringType(), True),
            StructField('j', BooleanType(), True),
            StructField('k', BinaryType(), True),
            StructField('y', DecimalType(10, 0), True),
            StructField('z', DateType(), True),
            StructField('dd', TimestampType(), True),
            StructField('hh', DayTimeIntervalType(0, 3), True),
            StructField('ii', IntegerType(), True),
        ]
    )
    actual_schema = OptionalsModel.model_spark_schema()
    assert actual_schema == expected_schema
