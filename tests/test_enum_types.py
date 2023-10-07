from enum import Enum, IntEnum

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkdantic.model import SparkModel


class IntTestEnum(IntEnum):
    X = 1
    Y = 2


class StrTestEnum(str, Enum):
    A = 'a'
    B = 'b'


class EnumValuesModel(SparkModel):
    a: IntTestEnum
    b: StrTestEnum


def test_enum_values():
    expected_schema = StructType(
        [
            StructField('a', IntegerType(), False),
            StructField('b', StringType(), False),
        ]
    )
    generated_schema = EnumValuesModel.model_spark_schema()
    assert generated_schema == expected_schema
