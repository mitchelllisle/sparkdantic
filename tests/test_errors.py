from enum import Enum

import pytest

from sparkdantic import SparkModel
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


def test_unsupported_enum_type_raises_error():
    class ClassicEnum(Enum):
        this = 'bad'

    class ClassicEnumModel(SparkModel):
        e: ClassicEnum

    with pytest.raises(FieldConversionError) as exc_info:
        ClassicEnumModel.model_spark_schema()

    assert 'Error converting field `e` to PySpark type' in str(exc_info.value)
    # Check cause
    assert isinstance(exc_info.value.__cause__, TypeError)
    assert f'Enum {ClassicEnum} is not supported. Only int and str mixins are supported.' in str(
        exc_info.value.__cause__
    )

    class FloatEnum(float, Enum):
        this = 3.14

    class FloatEnumModel(SparkModel):
        e: FloatEnum

    with pytest.raises(FieldConversionError) as exc_info:
        FloatEnumModel.model_spark_schema()

    # Check cause
    assert isinstance(exc_info.value.__cause__, TypeError)
    assert f'Enum {FloatEnum} is not supported. Only int and str mixins are supported.' in str(
        exc_info.value.__cause__
    )
