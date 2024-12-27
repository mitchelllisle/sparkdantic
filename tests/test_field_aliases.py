import pytest
from pydantic import AliasChoices, AliasPath, Field
from pyspark.sql.types import IntegerType, StructField, StructType

from sparkdantic import SparkModel


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


def test_invalid_json_schema_mode_raises_error():
    with pytest.raises(ValueError) as exc_info:
        AliasModel.model_spark_schema(mode='invalid_mode')

    assert "`mode` must be one of ('validation', 'serialization')" in str(exc_info.value)
