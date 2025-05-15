from enum import Enum, IntEnum

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkdantic import SparkModel
from sparkdantic.exceptions import TypeConversionError


def test_supported_enum_fields(spark: SparkSession):
    class IntegerEnum(IntEnum):
        X = 1
        Y = 2

    class StringEnum(str, Enum):
        A = 'a'
        B = 'b'

    class EnumModel(SparkModel):
        a: IntegerEnum
        b: StringEnum

    expected_schema = StructType(
        [
            StructField('a', IntegerType(), False),
            StructField('b', StringType(), False),
        ]
    )
    actual_schema = EnumModel.model_spark_schema()
    assert actual_schema == expected_schema

    df = spark.createDataFrame([], schema=actual_schema)
    ddl_schema = EnumModel.model_ddl_spark_schema()
    assert ddl_schema == df._jdf.schema().toDDL()


def test_unsupported_enum_type_raises_error():
    class ClassicEnum(Enum):
        this = 'bad'

    class ClassicEnumModel(SparkModel):
        e: ClassicEnum

    with pytest.raises(TypeConversionError) as exc_info:
        ClassicEnumModel.model_spark_schema()

    assert 'Error converting field `e` to PySpark type' in str(exc_info.value)
    # Check cause
    assert isinstance(exc_info.value.__cause__, TypeError)
    assert f'Enum {ClassicEnum} is not supported. Only int and str mixins are supported.' in str(
        exc_info.value.__cause__
    )

    class FloatEnum(float, Enum):
        this = 3.14

    class FloatEnumModel(SparkModel):
        e: FloatEnum

    with pytest.raises(TypeConversionError) as exc_info:
        FloatEnumModel.model_spark_schema()

    # Check cause
    assert isinstance(exc_info.value.__cause__, TypeError)
    assert f'Enum {FloatEnum} is not supported. Only int and str mixins are supported.' in str(
        exc_info.value.__cause__
    )
