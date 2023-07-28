import pytest

from sparkdantic.model import SparkModel


class BadListType(SparkModel):
    values: list


class NewType:
    pass


class BadType(SparkModel):
    t: NewType


def test_value_error():
    with pytest.raises(TypeError):
        BadListType(values=[1, 2]).spark_schema()


def test_bad_type():
    with pytest.raises(TypeError):
        BadType(t=NewType()).spark_schema()
