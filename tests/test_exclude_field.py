from typing import Any

import pytest
from pydantic import Field
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkdantic.exceptions import TypeConversionError
from sparkdantic.model import SparkModel


def test_exclude():
    class ModelWithIncompatibleTypes(SparkModel):
        a: int
        b: Any = Field(exclude=True)
        c: str
        d: float = Field(exclude=True)

    expected_schema = StructType(
        [
            StructField('a', IntegerType(), False),
            StructField('c', StringType(), False),
        ]
    )
    generated_schema = ModelWithIncompatibleTypes.model_spark_schema(exclude_fields=True)
    assert generated_schema == expected_schema
    # Default is to include all fields, so should raise TypeConversionError
    with pytest.raises(TypeConversionError):
        ModelWithIncompatibleTypes.model_spark_schema()
    with pytest.raises(TypeConversionError):
        ModelWithIncompatibleTypes.model_spark_schema(exclude_fields=False)


def test_nested_exclude():
    class ModelWithIncompatibleTypes(SparkModel):
        a: int
        b: Any = Field(exclude=True)
        c: str
        d: float = Field(exclude=True)

    class ModelWithIncompatibleNestedTypes(SparkModel):
        a: ModelWithIncompatibleTypes

    expected_schema = StructType(
        [
            StructField(
                'a',
                StructType(
                    [
                        StructField('a', IntegerType(), False),
                        StructField('c', StringType(), False),
                    ]
                ),
                nullable=False,
            )
        ]
    )
    generated_schema = ModelWithIncompatibleNestedTypes.model_spark_schema(exclude_fields=True)
    assert generated_schema == expected_schema
    # Default is to include all fields, so should raise TypeConversionError
    with pytest.raises(TypeConversionError):
        ModelWithIncompatibleNestedTypes.model_spark_schema()
    with pytest.raises(TypeConversionError):
        ModelWithIncompatibleNestedTypes.model_spark_schema(exclude_fields=False)
