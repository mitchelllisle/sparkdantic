from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sparkdantic import SparkModel


class InnerModel(SparkModel):
    name: str = 'test'
    age: int = 50


class RecursiveModel(SparkModel):
    details: InnerModel
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
                True,
            ),
            StructField('created', StringType(), False),
        ]
    )
    inner = InnerModel()
    rec = RecursiveModel(details=inner)
    schema = rec.model_spark_schema()
    assert schema == expected
