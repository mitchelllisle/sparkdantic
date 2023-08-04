from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List

from hypothesis import given
from hypothesis import strategies as st
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


class MyModel(SparkModel):
    k: str


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
    oo: List[MyModel]


list_values_strategy = st.fixed_dictionaries(
    {
        'm': st.lists(st.integers(min_value=1, max_value=1000)),
        'n': st.lists(st.floats(min_value=0, max_value=5)),
        'o': st.lists(st.text(min_size=1, max_size=20)),
        'p': st.lists(st.booleans()),
        'q': st.lists(st.binary(max_size=100)),
        'r': st.lists(
            st.decimals(min_value=0, max_value=1000, allow_nan=False, allow_infinity=False)
        ),
        'aa': st.lists(st.dates()),
        'ee': st.lists(st.datetimes()),
        'ii': st.lists(st.timedeltas()),
        'oo': st.lists(st.builds(MyModel, k=st.text(min_size=1, max_size=20)))
    }
)


@given(list_values_strategy)
def test_list_values(data):
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
            StructField('oo', ArrayType(StructType([StructField("k", StringType(), False)]), False), False),
        ]
    )
    user = ListValuesModel(**data)
    generated_schema = user.model_spark_schema()
    assert generated_schema == expected_schema
