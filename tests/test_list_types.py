from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import IntEnum
from typing import List, Optional, Union

from pydantic import BaseModel
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

from sparkdantic import SparkModel


def test_list_fields():
    class IntegerEnum(IntEnum):
        X = 1
        Y = 2

    class InnerSparkModel(SparkModel):
        k: str

    class InnerBaseModel(BaseModel):
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
        oo: List[InnerSparkModel]
        o1: Optional[List[Optional[InnerSparkModel]]]
        ob1: Optional[List[InnerBaseModel]]
        o2: Optional[List[IntegerEnum]]
        o3: list[str]
        o4: List[Optional[InnerSparkModel]]
        ob4: List[Optional[InnerBaseModel]]
        j: list[Union[str, None]]

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
            StructField(
                'oo', ArrayType(StructType([StructField('k', StringType(), False)]), False), False
            ),
            StructField(
                'o1', ArrayType(StructType([StructField('k', StringType(), False)]), True), True
            ),
            StructField(
                'ob1', ArrayType(StructType([StructField('k', StringType(), False)]), False), True
            ),
            StructField('o2', ArrayType(IntegerType(), False), True),
            StructField('o3', ArrayType(StringType(), False), False),
            StructField(
                'o4', ArrayType(StructType([StructField('k', StringType(), False)]), True), False
            ),
            StructField(
                'ob4', ArrayType(StructType([StructField('k', StringType(), False)]), True), False
            ),
            StructField('j', ArrayType(StringType(), True), False),
        ]
    )
    actual_schema = ListValuesModel.model_spark_schema()
    assert actual_schema == expected_schema
