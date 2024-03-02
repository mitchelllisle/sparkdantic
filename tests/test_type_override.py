import pytest
from pydantic import UUID4, Field
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkdantic import SparkModel


class MyModel(SparkModel):
    id: UUID4 = Field(spark_type=StringType)
    t: str = Field(spark_type=IntegerType)


def test_override():
    schema = MyModel.model_spark_schema()
    expected = StructType(
        [StructField('id', StringType(), False), StructField('t', IntegerType(), False)]
    )
    assert schema == expected


def test_bad_override():
    class BadOverride(SparkModel):
        id: UUID4 = Field(spark_type='a')

    with pytest.raises(TypeError):
        BadOverride.model_spark_schema()
