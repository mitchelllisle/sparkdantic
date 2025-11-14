from pyspark.sql import types as T

from sparkdantic import SparkField, SparkModel


class Model(SparkModel):
    z: int = SparkField(spark_type=T.StringType, alias='_z')
    zz: int
    zzz: str = SparkField(spark_type=T.VarcharType(100))


def test_spark_field():
    schema = Model.model_spark_schema()
    assert schema == T.StructType(
        [
            T.StructField('_z', T.StringType(), False),
            T.StructField('zz', T.IntegerType(), False),
            T.StructField('zzz', T.VarcharType(100), False),
        ]
    )
