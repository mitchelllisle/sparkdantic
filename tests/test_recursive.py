from typing import List, Optional

import pytest
from pydantic import BaseModel
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

from sparkdantic import SparkModel

SUPPORTED_SUB_CLASSES = [SparkModel, BaseModel]


@pytest.mark.parametrize('sub_class', SUPPORTED_SUB_CLASSES)
def test_recursive(sub_class):
    class InnerModel(sub_class):
        name: str = 'test'
        age: int = 50

    class RecursiveModel(SparkModel):
        details: InnerModel
        created: str = 'today'

    expected = StructType(
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
    inner = InnerModel()
    rec = RecursiveModel(details=inner)
    schema = rec.model_spark_schema()
    assert schema == expected


@pytest.mark.parametrize('sub_class', SUPPORTED_SUB_CLASSES)
def test_optional_child_classes(sub_class):
    class ChildSchema(sub_class):
        ncol1: str
        ncol2: Optional[str] = None
        ncol3: Optional[List[str]] = None

    class ParentSchema(SparkModel):
        col1: Optional[ChildSchema] = None

    schema = ParentSchema.model_spark_schema()
    expected = StructType(
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
    assert schema == expected
