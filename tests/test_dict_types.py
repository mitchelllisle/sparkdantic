from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict

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
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sparkdantic.model import SparkModel


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


dict_values_strategy = st.fixed_dictionaries(
    {
        's': st.dictionaries(
            keys=st.integers(min_value=1, max_value=1000),
            values=st.integers(min_value=1, max_value=1000),
        ),
        't': st.dictionaries(
            keys=st.floats(min_value=0, max_value=5), values=st.floats(min_value=0, max_value=5)
        ),
        'u': st.dictionaries(
            keys=st.text(min_size=1, max_size=20), values=st.text(min_size=1, max_size=20)
        ),
        'v': st.dictionaries(keys=st.booleans(), values=st.booleans()),
        'w': st.dictionaries(keys=st.binary(max_size=100), values=st.binary(max_size=100)),
        'x': st.dictionaries(
            keys=st.decimals(min_value=0, max_value=1000, allow_nan=False, allow_infinity=False),
            values=st.decimals(min_value=0, max_value=1000, allow_nan=False, allow_infinity=False),
        ),
        'bb': st.dictionaries(keys=st.dates(), values=st.dates()),
        'ff': st.dictionaries(keys=st.datetimes(), values=st.datetimes()),
        'jj': st.dictionaries(keys=st.timedeltas(), values=st.timedeltas()),
    }
)


@given(dict_values_strategy)
def test_dict_values(data):
    expected_schema = StructType(
        [
            StructField('s', MapType(IntegerType(), IntegerType(), False), True),
            StructField('t', MapType(DoubleType(), DoubleType(), False), True),
            StructField('u', MapType(StringType(), StringType(), False), True),
            StructField('v', MapType(BooleanType(), BooleanType(), False), True),
            StructField('w', MapType(BinaryType(), BinaryType(), False), True),
            StructField('x', MapType(DecimalType(10, 0), DecimalType(10, 0), False), True),
            StructField('bb', MapType(DateType(), DateType(), False), True),
            StructField('ff', MapType(TimestampType(), TimestampType(), False), True),
            StructField(
                'jj', MapType(DayTimeIntervalType(0, 3), DayTimeIntervalType(0, 3), False), True
            ),
        ]
    )
    user = DictValuesModel(**data)
    generated_schema = user.spark_schema()
    assert generated_schema == expected_schema
