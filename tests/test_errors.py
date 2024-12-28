from typing import Literal

import pytest

from sparkdantic import SparkModel, create_spark_schema
from sparkdantic.exceptions import FieldConversionError


def test_incomplete_type_annotations_raise_error():
    class BadListType(SparkModel):
        values: list

    with pytest.raises(FieldConversionError) as exc_info:
        BadListType.model_spark_schema()

    assert 'Error converting field `values` to PySpark type' in str(exc_info.value)
    # Check cause
    assert isinstance(exc_info.value.__cause__, TypeError)
    assert 'list type must have a type argument' in str(exc_info.value.__cause__)

    class BadDictType(SparkModel):
        mapping: dict

    with pytest.raises(FieldConversionError) as exc_info:
        BadDictType.model_spark_schema()

    # Check cause
    assert 'dict type must have key and value type arguments' in str(exc_info.value.__cause__)


def test_user_defined_field_type_raises_error():
    class UserDefinedType:
        ...

    class UserDefinedModel(SparkModel):
        t: UserDefinedType

    with pytest.raises(FieldConversionError) as exc_info:
        UserDefinedModel.model_spark_schema()

    assert 'Error converting field `t` to PySpark type' in str(exc_info.value)
    # Check cause
    assert isinstance(exc_info.value.__cause__, TypeError)
    assert f'Type {UserDefinedType} not recognized' in str(exc_info.value.__cause__)


def test_create_spark_schema_raises_error_for_invalid_type():
    class NotAModel:
        a: int

    with pytest.raises(TypeError) as exc_info:
        create_spark_schema(NotAModel)

    assert '`model` must be of type `SparkModel` or `pydantic.BaseModel`' in str(exc_info.value)


def test_literal_with_inconsistent_type_arguments_raises_error():
    class BadLiteralModel(SparkModel):
        mixed_types: Literal['yes', 1]

    with pytest.raises(FieldConversionError) as exc_info:
        BadLiteralModel.model_spark_schema()

    # Check cause
    assert isinstance(exc_info.value.__cause__, TypeError)
    assert (
        'Multiple types detected in `Literal` type. Only one consistent arg type is supported.'
        in str(exc_info.value.__cause__)
    )
