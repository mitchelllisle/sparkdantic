from typing import List, Optional

import pytest
from pydantic import BaseModel
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

from sparkdantic import SparkModel

SUPPORTED_SUB_CLASSES = [SparkModel, BaseModel]


@pytest.mark.parametrize('sub_class', SUPPORTED_SUB_CLASSES)
def test_nested_models(sub_class):
    class InnerModel(sub_class):
        name: str = 'test'
        age: int = 50

    class OuterModel(SparkModel):
        details: InnerModel
        created: str = 'today'

    expected_schema = StructType(
        [
            StructField(
                'details',
                StructType(
                    [
                        StructField('name', StringType(), False),
                        StructField('age', IntegerType(), False),
                    ]
                ),
                False,
            ),
            StructField('created', StringType(), False),
        ]
    )
    actual_schema = OuterModel.model_spark_schema()
    assert actual_schema == expected_schema


@pytest.mark.parametrize('sub_class', SUPPORTED_SUB_CLASSES)
def test_optional_nested_models(sub_class):
    class InnerModel(sub_class):
        ncol1: str
        ncol2: Optional[str] = None
        ncol3: Optional[List[str]] = None

    class OuterModel(SparkModel):
        col1: Optional[InnerModel] = None

    actual_schema = OuterModel.model_spark_schema()
    expected_schema = StructType(
        [
            StructField(
                'col1',
                dataType=StructType(
                    [
                        StructField('ncol1', StringType(), False),
                        StructField('ncol2', StringType(), True),
                        StructField('ncol3', ArrayType(StringType(), False), True),
                    ]
                ),
                nullable=True,
            )
        ]
    )
    assert actual_schema == expected_schema
