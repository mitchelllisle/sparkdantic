from datetime import date, datetime, timedelta
from decimal import Decimal

from pydantic import SecretBytes, SecretStr
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
