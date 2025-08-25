from typing import Annotated, Optional

from pydantic import BaseModel, computed_field
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from sparkdantic import create_spark_schema, SparkField


class ComputedOnlyModel(BaseModel):
    @computed_field
    @property
    def a(self) -> int:
        return 1

    @computed_field
    @property
    def b(self) -> Optional[str]:
        return 'b'


class ComputedOnlyModelWithAliases(ComputedOnlyModel):
    @computed_field(alias='_b')
    @property
    def b(self) -> int:
        return 2

    @computed_field(alias='C')
    @property
    def c(self) -> int:
        return 3


def test_computed_field_is_included_in_schema_when_using_serialization_mode():
    expected_schema = StructType(
        [
            StructField('a', IntegerType(), False),
            StructField('b', StringType(), True),
        ]
    )

    actual_schema = create_spark_schema(ComputedOnlyModel, mode='serialization')
    assert actual_schema == expected_schema


def test_computed_field_is_not_included_in_schema_when_using_validation_mode():
    expected_schema = StructType()

    actual_schema = create_spark_schema(ComputedOnlyModel, mode='validation')
    assert actual_schema == expected_schema


def test_computed_field_is_included_in_schema_when_inherited():
    class ComputedInheritedModel(ComputedOnlyModel):
        @computed_field
        @property
        def b(self) -> str:
            return 'b'

    expected_schema = StructType(
        [
            StructField('a', IntegerType(), False),
            StructField('b', StringType(), False),
        ]
    )
    actual_schema = create_spark_schema(ComputedInheritedModel, mode='serialization')
    assert actual_schema == expected_schema


def test_computed_fields_with_aliases():
    expected_schema = StructType(
        [
            StructField('a', IntegerType(), False),
            StructField('_b', IntegerType(), False),
            StructField('C', IntegerType(), False),
        ]
    )

    actual_schema = create_spark_schema(ComputedOnlyModelWithAliases, mode='serialization')
    assert actual_schema == expected_schema


def test_computed_field_with_return_type():
    class ComputedWithReturnType(BaseModel):
        @computed_field(return_type=str)
        @property
        def d(self) -> int:
            return 4

    expected_schema = StructType(
        [
            StructField('d', StringType(), False),
        ]
    )
    actual_schema = create_spark_schema(ComputedWithReturnType, mode='serialization')
    assert actual_schema == expected_schema


def test_computed_field_with_spark_type():
    class ComputedWithSparkType(BaseModel):
        @computed_field(json_schema_extra={"spark_type": LongType})
        @property
        def d(self) -> int:
            return 4

    expected_schema = StructType(
        [
            StructField('d', LongType(), False),
        ]
    )
    actual_schema = create_spark_schema(ComputedWithSparkType, mode='serialization')
    assert actual_schema == expected_schema


def test_computed_field_with_annotated_return_type():
    class ComputedWithReturnSparkType(BaseModel):
        @computed_field
        @property
        def d(self) -> Annotated[int, SparkField(spark_type=LongType)]:
            return 4

    expected_schema = StructType(
        [
            StructField('d', LongType(), False),
        ]
    )
    actual_schema = create_spark_schema(ComputedWithReturnSparkType, mode='serialization')
    assert actual_schema == expected_schema


def test_computed_field_with_spark_type_over_annotated_return():
    class ComputedWithSparkType(BaseModel):
        @computed_field(json_schema_extra={"spark_type": LongType})
        @property
        def d(self) -> Annotated[int, SparkField(spark_type=StringType)]:
            return 4

    expected_schema = StructType(
        [
            StructField('d', LongType(), False),
        ]
    )
    actual_schema = create_spark_schema(ComputedWithSparkType, mode='serialization')
    assert actual_schema == expected_schema
