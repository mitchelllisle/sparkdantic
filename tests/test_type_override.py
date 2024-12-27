from typing import Union

import pytest
from pydantic import UUID4, Field
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from sparkdantic import SparkModel


class MyModel(SparkModel):
    id: UUID4 = Field(spark_type=StringType)
    t: str = Field(spark_type=IntegerType)
    o: Union[int, None] = Field(spark_type=LongType)


def test_override():
    schema = MyModel.model_spark_schema()
    expected = StructType(
        [
            StructField('id', StringType(), False),
            StructField('t', IntegerType(), False),
            StructField('o', LongType(), True),
        ]
    )
    assert schema == expected


def test_bad_override():
    class BadOverride(SparkModel):
        id: UUID4 = Field(spark_type='a')

    with pytest.raises(TypeError) as exc_info:
        BadOverride.model_spark_schema()

    assert '`spark_type` override should be a `pyspark.sql.types.DataType`' in str(exc_info.value)
