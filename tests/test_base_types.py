from datetime import date, datetime, timedelta
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st
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


raw_values_strategy = st.fixed_dictionaries(
    {
        'a': st.integers(min_value=1, max_value=1000),
        'b': st.floats(min_value=0, max_value=5),
        'c': st.text(min_size=1, max_size=20),
        'd': st.booleans(),
        'e': st.binary(max_size=100),
        'f': st.decimals(min_value=0, max_value=1000, allow_nan=False, allow_infinity=False),
        'y': st.dates(),
        'cc': st.datetimes(),
        'gg': st.timedeltas(),
    }
)


@given(raw_values_strategy)
def test_raw_values(data):
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
        ]
    )
    user = RawValuesModel(**data)
    generated_schema = user.model_spark_schema()
    assert generated_schema == expected_schema
