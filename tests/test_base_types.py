from sparkdantic.model import SparkModel
from hypothesis import given, strategies as st
from pyspark.sql.types import (
    IntegerType,
    StringType,
    BinaryType,
    BooleanType,
    TimestampType,
    DecimalType,
    DayTimeIntervalType,
    DateType,
    DoubleType,
    StructType,
    StructField,
)

from decimal import Decimal
from datetime import date, datetime, timedelta


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


raw_values_strategy = st.fixed_dictionaries({
    'a': st.integers(min_value=1, max_value=1000),
    'b': st.floats(min_value=0, max_value=5),
    'c': st.text(min_size=1, max_size=20),
    'd': st.booleans(),
    'e': st.binary(max_size=100),
    'f': st.decimals(min_value=0, max_value=1000, allow_nan=False, allow_infinity=False),
    'y': st.dates(),
    'cc': st.datetimes(),
    'gg': st.timedeltas(),
})


@given(raw_values_strategy)
def test_raw_values(data):
    expected_schema = StructType([StructField('a', IntegerType(), True), StructField('b', DoubleType(), True), StructField('c', StringType(), True), StructField('d', BooleanType(), True), StructField('e', BinaryType(), True), StructField('f', DecimalType(10,0), True), StructField('y', DateType(), True), StructField('cc', TimestampType(), True), StructField('gg', DayTimeIntervalType(0, 3), True)])
    user = RawValuesModel(**data)
    generated_schema = user.spark_schema()
    assert generated_schema == expected_schema
