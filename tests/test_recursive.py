from typing import Optional

from pydantic import BaseModel
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkdantic import SparkModel


class InnerModelBase(BaseModel):
    name: str = 'test'
    age: int = 50


class InnerModel(SparkModel):
    name: str = 'test'
    age: int = 50


class RecursiveModel(SparkModel):
    details: InnerModel
    created: str = 'today'


class RecursiveModelBase(SparkModel):
    details: InnerModelBase
    created: str = 'today'


def test_recursive():
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


def test_recursive_base_model():
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
    inner = InnerModelBase()
    rec = RecursiveModelBase(details=inner)
    schema = rec.model_spark_schema()
    assert schema == expected


def test_optional_child_classes():
    class ChildSchema(SparkModel):
        ncol1: str
        ncol2: Optional[str] = None

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
                    ]
                ),
                nullable=True,
            )
        ]
    )
    assert schema == expected
