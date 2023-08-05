from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List

from pyspark.sql.types import (
    ArrayType,
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


class ListValuesModel(SparkModel):
    m: List[int]
    n: List[float]
    o: List[str]
    p: List[bool]
    q: List[bytes]
    r: List[Decimal]
    aa: List[date]
    ee: List[datetime]
    ii: List[timedelta]


def test_list_values():
    expected_schema = StructType(
        [
            StructField('m', ArrayType(IntegerType(), False), False),
            StructField('n', ArrayType(DoubleType(), False), False),
            StructField('o', ArrayType(StringType(), False), False),
            StructField('p', ArrayType(BooleanType(), False), False),
            StructField('q', ArrayType(BinaryType(), False), False),
            StructField('r', ArrayType(DecimalType(10, 0), False), False),
            StructField('aa', ArrayType(DateType(), False), False),
            StructField('ee', ArrayType(TimestampType(), False), False),
            StructField('ii', ArrayType(DayTimeIntervalType(0, 3), False), False),
        ]
    )
    generated_schema = ListValuesModel.model_spark_schema()
    assert generated_schema == expected_schema
