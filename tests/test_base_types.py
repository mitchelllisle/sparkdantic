from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Literal, Optional
from uuid import UUID

import pytest
from pydantic import AliasChoices, AliasPath, BaseModel, Field, SecretBytes, SecretStr
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sparkdantic.model import SparkModel, create_spark_schema


class DecimalModel(SparkModel):
    a: Decimal
    b: Decimal = Field(decimal_places=2)
    c: Decimal = Field(decimal_places=2, max_digits=5)


class RawValuesModel(SparkModel):
    a: int
    b: float
    c: str
    d: bool
    e: bytes
    f: Decimal
    y: date
    cc: datetime
    gg: timedelta
    hh: DoubleType
    ii: SecretBytes
    jj: SecretStr
    x: str = Field(alias='_x')
    uuid: UUID


class NestedAliasModel(SparkModel):
    z: int = Field(alias='_z')


class AliasModel(SparkModel):
    a: int = Field(alias='_a')
    b: int = Field(serialization_alias='_b')
    c: int = Field(validation_alias='_c')
    d: int
    _e: int  # Omitted from the model schema
    f: NestedAliasModel = Field(alias='_f')
    g: int = Field(alias='_g', serialization_alias='__g')
    h: int = Field(alias='_h', serialization_alias='h_s', validation_alias='h_v')
    i: int = Field(validation_alias=AliasChoices('i_first', 'i_second'))
    j: int = Field(validation_alias=AliasPath('j_array', 0))


def test_raw_values():
    expected_schema = StructType(
        [
            StructField('a', IntegerType(), False),
            StructField('b', DoubleType(), False),
            StructField('c', StringType(), False),
            StructField('d', BooleanType(), False),
            StructField('e', BinaryType(), False),
            StructField('f', DecimalType(10, 0), False),
            StructField('y', DateType(), False),
            StructField('cc', TimestampType(), False),
            StructField('gg', DayTimeIntervalType(0, 3), False),
            StructField('hh', DoubleType(), False),
            StructField('ii', BinaryType(), False),
            StructField('jj', StringType(), False),
            StructField('_x', StringType(), False),
            StructField('uuid', StringType(), False),
        ]
    )
    generated_schema = RawValuesModel.model_spark_schema(by_alias=True)
    assert generated_schema == expected_schema


def test_literal():
    expected = StructType(
        [
            StructField('is_this_a_field', StringType(), False),
            StructField('is_this_another_field', StringType(), True),
        ]
    )

    class MyClass(BaseModel):
        is_this_a_field: Literal['yes', 'no']

    class SparkMyClass(SparkModel, MyClass):
        is_this_another_field: Optional[Literal['yes', 'no']]

    schema = SparkMyClass.model_spark_schema()
    assert schema == expected


def test_inconsistent_literal():
    class MyClass(SparkModel):
        is_this_a_field: Literal['yes', 1]

    with pytest.raises(TypeError):
        MyClass.model_spark_schema()


def test_annotated_type():
    class MyClass(SparkModel):
        optional_field: Optional[Annotated[int, Field(lt=1, gt=10)]]
        required_field: Annotated[int, Field(lt=1, gt=10)]

    schema = MyClass.model_spark_schema()
    assert schema == StructType(
        [
            StructField('optional_field', IntegerType(), True),
            StructField('required_field', IntegerType(), False),
        ]
    )


def test_decimal_types():
    expected_schema = StructType(
        [
            StructField('a', DecimalType(10, 0), False),
            StructField('b', DecimalType(10, 2), False),
            StructField('c', DecimalType(5, 2), False),
        ]
    )
    generated_schema = DecimalModel.model_spark_schema()
    assert generated_schema == expected_schema


def test_safe_casting():
    class MyClass(SparkModel):
        a: int
        b: Optional[int]
        c: Optional[int] = None
        d: Optional[int] = Field(spark_type=StringType)
        e: str = Field(spark_type=IntegerType)
        f: str

    schema = MyClass.model_spark_schema(safe_casting=True)
    assert schema == StructType(
        [
            StructField('a', LongType(), False),
            StructField('b', LongType(), True),
            StructField('c', LongType(), True),
            StructField('d', StringType(), True),
            StructField('e', IntegerType(), False),
            StructField('f', StringType(), False),
        ]
    )


def test_spark_schema_is_created_for_basemodel():
    class MyModel(BaseModel):
        a: int

    schema = create_spark_schema(MyModel)

    assert schema == StructType(
        [
            StructField('a', IntegerType(), False),
        ]
    )


def test_create_spark_schema_raises_error_for_invalid_type():
    class NotAModel:
        a: int

    with pytest.raises(TypeError) as exc_info:
        create_spark_schema(NotAModel)

    assert '`model` must be of type `SparkModel` or `pydantic.BaseModel`' in str(exc_info.value)


def test_spark_schema_contains_validation_field_aliases_by_default():
    schema = AliasModel.model_spark_schema()
    assert schema == StructType(
        [
            StructField('_a', IntegerType(), False),
            StructField('b', IntegerType(), False),
            StructField('_c', IntegerType(), False),
            StructField('d', IntegerType(), False),
            StructField('_f', StructType([StructField('_z', IntegerType(), False)]), False),
            StructField('_g', IntegerType(), False),
            StructField('h_v', IntegerType(), False),
            StructField('i_first', IntegerType(), False),
            StructField('j', IntegerType(), False),
        ]
    )


def test_spark_schema_contains_field_names_when_not_using_aliases():
    schema = AliasModel.model_spark_schema(by_alias=False)
    assert schema == StructType(
        [
            StructField('a', IntegerType(), False),
            StructField('b', IntegerType(), False),
            StructField('c', IntegerType(), False),
            StructField('d', IntegerType(), False),
            StructField('f', StructType([StructField('z', IntegerType(), False)]), False),
            StructField('g', IntegerType(), False),
            StructField('h', IntegerType(), False),
            StructField('i', IntegerType(), False),
            StructField('j', IntegerType(), False),
        ]
    )


def test_spark_schema_contains_serialization_aliases_when_using_serialization_mode():
    schema = AliasModel.model_spark_schema(mode='serialization')
    assert schema == StructType(
        [
            StructField('_a', IntegerType(), False),
            StructField('_b', IntegerType(), False),
            StructField('c', IntegerType(), False),
            StructField('d', IntegerType(), False),
            StructField('_f', StructType([StructField('_z', IntegerType(), False)]), False),
            StructField('__g', IntegerType(), False),
            StructField('h_s', IntegerType(), False),
            StructField('i', IntegerType(), False),
            StructField('j', IntegerType(), False),
        ]
    )


def test_spark_schema_contains_field_names_when_using_serialization_mode_and_not_using_aliases():
    schema = AliasModel.model_spark_schema(by_alias=False, mode='serialization')
    assert schema == StructType(
        [
            StructField('a', IntegerType(), False),
            StructField('b', IntegerType(), False),
            StructField('c', IntegerType(), False),
            StructField('d', IntegerType(), False),
            StructField('f', StructType([StructField('z', IntegerType(), False)]), False),
            StructField('g', IntegerType(), False),
            StructField('h', IntegerType(), False),
            StructField('i', IntegerType(), False),
            StructField('j', IntegerType(), False),
        ]
    )


@pytest.mark.parametrize(
    'by_alias, mode',
    [
        (True, 'validation'),
        (False, 'validation'),
        (True, 'serialization'),
        (False, 'serialization'),
    ],
)
def test_spark_model_schema_json_has_same_field_names_to_model_json_schema(by_alias, mode):
    spark_schema = AliasModel.model_spark_schema(by_alias=by_alias, mode=mode)
    json_schema = AliasModel.model_json_schema(by_alias=by_alias, mode=mode)

    assert sorted(spark_schema.fieldNames()) == sorted(json_schema['properties'].keys())
