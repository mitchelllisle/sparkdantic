from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

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


optional_values_strategy = st.fixed_dictionaries(
    {
        'g': st.one_of(st.integers(min_value=1, max_value=1000), st.none()),
        'h': st.one_of(st.floats(min_value=0, max_value=5), st.none()),
        'i': st.one_of(st.text(min_size=1, max_size=20), st.none()),
        'j': st.one_of(st.booleans(), st.none()),
        'k': st.one_of(st.binary(max_size=100), st.none()),
        'l': st.one_of(
            st.decimals(min_value=0, max_value=1000, allow_nan=False, allow_infinity=False),
            st.none(),
        ),
        'z': st.one_of(st.dates(), st.none()),
        'dd': st.one_of(st.datetimes(), st.none()),
        'hh': st.one_of(st.timedeltas(), st.none()),
    }
)


@given(optional_values_strategy)
def test_optional_values(data):
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
    user = OptionalValuesModel(**data)
    generated_schema = user.model_spark_schema()
    assert generated_schema == expected_schema
