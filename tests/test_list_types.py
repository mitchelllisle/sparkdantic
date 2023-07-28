from sparkdantic.model import SparkModel
from hypothesis import given, strategies as st
from pyspark.sql.types import (
    IntegerType,
    StringType,
    BinaryType,
    BooleanType,
    ArrayType,
    TimestampType,
    DecimalType,
    DayTimeIntervalType,
    DateType,
    DoubleType,
    StructType,
    StructField,
)

from typing import List
from decimal import Decimal
from datetime import date, datetime, timedelta


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


list_values_strategy = st.fixed_dictionaries({
    'm': st.lists(st.integers(min_value=1, max_value=1000)),
    'n': st.lists(st.floats(min_value=0, max_value=5)),
    'o': st.lists(st.text(min_size=1, max_size=20)),
    'p': st.lists(st.booleans()),
    'q': st.lists(st.binary(max_size=100)),
    'r': st.lists(st.decimals(min_value=0, max_value=1000, allow_nan=False, allow_infinity=False)),
    'aa': st.lists(st.dates()),
    'ee': st.lists(st.datetimes()),
    'ii': st.lists(st.timedeltas()),
})


@given(list_values_strategy)
def test_list_values(data):
    expected_schema = StructType([StructField('m', ArrayType(IntegerType(), False), True), StructField('n', ArrayType(DoubleType(), False), True), StructField('o', ArrayType(StringType(), False), True), StructField('p', ArrayType(BooleanType(), False), True), StructField('q', ArrayType(BinaryType(), False), True), StructField('r', ArrayType(DecimalType(10,0), False), True), StructField('aa', ArrayType(DateType(), False), True), StructField('ee', ArrayType(TimestampType(), False), True), StructField('ii', ArrayType(DayTimeIntervalType(0, 3), False), True)])
    user = ListValuesModel(**data)
    generated_schema = user.spark_schema()
    assert generated_schema == expected_schema
