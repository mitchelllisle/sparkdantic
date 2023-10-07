from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import IntEnum
from typing import Dict

from pyspark.sql.types import (
    BinaryType,
    BooleanType,
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

from sparkdantic.model import SparkModel


class IntTestEnum(IntEnum):
    X = 1
    Y = 2


class DictValuesModel(SparkModel):
    s: Dict[int, int]
    t: Dict[float, float]
    u: Dict[str, str]
    v: Dict[bool, bool]
    w: Dict[bytes, bytes]
    x: Dict[Decimal, Decimal]
    bb: Dict[date, date]
    ff: Dict[datetime, datetime]
    jj: Dict[timedelta, timedelta]
    kk: Dict[IntTestEnum, IntTestEnum]


def test_dict_values():
    expected_schema = StructType(
        [
            StructField('s', MapType(IntegerType(), IntegerType(), False), False),
            StructField('t', MapType(DoubleType(), DoubleType(), False), False),
            StructField('u', MapType(StringType(), StringType(), False), False),
            StructField('v', MapType(BooleanType(), BooleanType(), False), False),
            StructField('w', MapType(BinaryType(), BinaryType(), False), False),
            StructField('x', MapType(DecimalType(10, 0), DecimalType(10, 0), False), False),
            StructField('bb', MapType(DateType(), DateType(), False), False),
            StructField('ff', MapType(TimestampType(), TimestampType(), False), False),
            StructField(
                'jj', MapType(DayTimeIntervalType(0, 3), DayTimeIntervalType(0, 3), False), False
            ),
            StructField('kk', MapType(IntegerType(), IntegerType(), False), False),
        ]
    )

    generated_schema = DictValuesModel.model_spark_schema()
    assert generated_schema == expected_schema
