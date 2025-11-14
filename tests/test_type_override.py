from typing import Union

import pytest
from pydantic import Field
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    VarcharType,
)

from sparkdantic import SparkModel
from sparkdantic.exceptions import TypeConversionError


def test_override():
    class MyModel(SparkModel):
        id: int = Field(spark_type=StringType)
        t: str = Field(spark_type=IntegerType)
        o: Union[int, None] = Field(spark_type=LongType)
        s: dict = Field(spark_type=StructType([StructField('a', StringType(), False)]))

    expected_schema = StructType(
        [
            StructField('id', StringType(), False),
            StructField('t', IntegerType(), False),
            StructField('o', LongType(), True),
            StructField('s', StructType([StructField('a', StringType(), False)]), False),
        ]
    )
    actual_schema = MyModel.model_spark_schema()
    assert actual_schema == expected_schema


def test_override_accepts_string_or_datatype():
    class SupporrtedOverride(SparkModel):
        s: int = Field(spark_type='string')
        i: str = Field(spark_type=IntegerType)

    expected_schema = StructType(
        [
            StructField('s', StringType(), False),
            StructField('i', IntegerType(), False),
        ]
    )
    actual_schema = SupporrtedOverride.model_spark_schema()
    assert actual_schema == expected_schema


def test_non_string_override_without_pyspark_raises_error():
    class UnsupportedOverride(SparkModel):
        id: str = Field(spark_type=int)

    with pytest.raises(TypeConversionError) as exc_info:
        UnsupportedOverride.model_spark_schema()

    assert 'Error converting field `id` to PySpark type' in str(exc_info.value)
    # Check cause
    assert (
        f'`spark_type` override should be a `str` type name (e.g. long) or `pyspark.sql.types.DataType` (e.g. LongType), but got {int}'
        in str(exc_info.value.__cause__)
    )


def test_spark_type_instance_override():
    """Test that DataType instances (not just classes) can be used as spark_type override."""

    class InstanceOverride(SparkModel):
        id: int = Field(spark_type=StringType())

    expected_schema = StructType([StructField('id', StringType(), False)])
    actual_schema = InstanceOverride.model_spark_schema()
    assert actual_schema == expected_schema


def test_parameterized_type_override():
    """Test that parameterized DataType instances like VarcharType(36) preserve their parameters."""

    class ParameterizedOverride(SparkModel):
        uuid: str = Field(spark_type=VarcharType(36))
        name: str = Field(spark_type=VarcharType(100))

    expected_schema = StructType(
        [
            StructField('uuid', VarcharType(36), False),
            StructField('name', VarcharType(100), False),
        ]
    )
    actual_schema = ParameterizedOverride.model_spark_schema()
    assert actual_schema == expected_schema
