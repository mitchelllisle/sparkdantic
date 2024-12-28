from typing import Union

import pytest
from pydantic import UUID4, Field
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from sparkdantic import SparkModel
from sparkdantic.exceptions import FieldConversionError


def test_override():
    class MyModel(SparkModel):
        id: int = Field(spark_type=StringType)
        t: str = Field(spark_type=IntegerType)
        o: Union[int, None] = Field(spark_type=LongType)

    schema = MyModel.model_spark_schema()
    expected = StructType(
        [
            StructField('id', StringType(), False),
            StructField('t', IntegerType(), False),
            StructField('o', LongType(), True),
        ]
    )
    assert schema == expected


def test_bad_override_raises_error():
    class BadOverride(SparkModel):
        id: UUID4 = Field(spark_type='a')

    with pytest.raises(FieldConversionError) as exc_info:
        BadOverride.model_spark_schema()

    assert 'Error converting field `id` to PySpark type' in str(exc_info.value)
    # Check cause
    assert '`spark_type` override should be a `pyspark.sql.types.DataType`' in str(
        exc_info.value.__context__
    )
