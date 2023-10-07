from enum import Enum

import pytest

from sparkdantic.model import SparkModel


class BadListType(SparkModel):
    values: list


class NewType:
    pass


class BadType(SparkModel):
    t: NewType


class BadEnum(Enum):
    this = 'bad'


class BadEnumType(SparkModel):
    e: BadEnum


def test_value_error():
    with pytest.raises(TypeError):
        BadListType(values=[1, 2]).model_spark_schema()


def test_bad_type():
    with pytest.raises(TypeError):
        BadType(t=NewType()).model_spark_schema()


def test_bad_enum_type():
    with pytest.raises(TypeError) as exc_info:
        BadEnumType(e=BadEnum.this.value).model_spark_schema()

    assert f'Enum {BadEnum} is not supported. Only int and str mixins are supported.' in str(
        exc_info.value
    )
