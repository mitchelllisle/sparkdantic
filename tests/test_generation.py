import datetime
from enum import Enum, IntEnum
from typing import Dict, List, Optional, Union, get_args, get_origin

import pytest
from faker import Faker
from pydantic import Field
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sparkdantic import ChoiceSpec, FuncSpec, MappingSpec, RangeSpec, SparkModel, ValueSpec


def _check_types_and_subtypes_match(field, row, name):
    container = get_origin(field.annotation)
    if container:
        inner = get_args(field.annotation)
        if container is not Union:
            if container is not dict:
                assert isinstance(row[name], container), f'{row[name]} is not of type {container}'
        else:
            assert any(
                [isinstance(row[name], t) for t in inner]
            ), f'{row[name]} is not of type {inner}'
    elif issubclass(field.annotation, Enum):
        assert row[name] in {
            item.value for item in field.annotation
        }, f'{row[name]} not in {field.annotation}'
    else:
        assert isinstance(
            row[name], field.annotation
        ), f'{row[name]} is not of type {field.annotation}'


class IntTestEnum(IntEnum):
    X = 1
    Y = 2


class SingleFieldModel(SparkModel):
    val: int


class SampleModel(SparkModel):
    logic_number: int
    logic: Optional[str]
    name: str
    age: Optional[int] = None
    jobs: List[str]
    map: Dict[str, str]
    single_value: int
    enum: IntTestEnum
    date: datetime.date
    datetime: datetime.datetime


def test_generator(spark: SparkSession, faker: Faker):
    n_rows = 100
    data = SampleModel.generate_data(
        spark,
        specs={
            'logic_number': RangeSpec(min_value=0, max_value=100),
            'logic': ChoiceSpec(values=['a', 'b', 'c']),
            'name': FuncSpec(func=faker.name),
            'age': RangeSpec(min_value=0, max_value=100, nullable=True),
            'single_value': ValueSpec(value=1),
        },
        n_rows=n_rows,
    )
    assert data.count() == n_rows


class AliasModel(SparkModel):
    val: int = Field(alias='_val')


def test_generate_data(spark: SparkSession):
    data_gen = SampleModel.generate_data(spark, n_rows=10).collect()
    assert data_gen is not None
    assert len(data_gen) == 10


def test_column_defaults(spark: SparkSession):
    data_gen = SampleModel.generate_data(spark, n_rows=1000).collect()
    for row in data_gen:
        for name, field in SampleModel.model_fields.items():
            _check_types_and_subtypes_match(field, row, name)


def test_columns_with_specs(spark: SparkSession, faker: Faker):
    n_rows = 1000
    names = [faker.name() for _ in range(100)]
    specs = {
        'name': ChoiceSpec(values=names),
        'age': RangeSpec(min_value=10, max_value=20),
        'jobs': ChoiceSpec(values=[faker.job(), faker.job()], n=2),
        'single_value': ValueSpec(value=1),
        'logic_number': ChoiceSpec(values=[1, 2, 3]),
        'logic': MappingSpec(mapping={1: 'a', 2: 'b'}, mapping_source='logic_number'),
        'enum': ChoiceSpec(values=[IntTestEnum.X.value]),
    }
    data_gen = SampleModel.generate_data(spark, specs=specs, n_rows=n_rows).collect()
    for row in data_gen:
        assert row.name in names
        assert row.age >= 10
        assert row.age <= 20
        assert row.single_value == 1
        if row.logic_number == 1:
            assert row.logic == 'a'
        if row.logic_number == 2:
            assert row.logic == 'b'
        if row.logic_number == 3:
            assert row.logic is None
        assert row.enum == IntTestEnum.X
        for name, field in SampleModel.model_fields.items():
            _check_types_and_subtypes_match(field, row, name)


def test_weights_validator_error():
    with pytest.raises(ValueError):
        ChoiceSpec(weights=[0.1], values=[1, 2])


def test_weights_validator():
    spec = ChoiceSpec(weights=[0.1, 0.9], values=[1, 2])
    assert sum(spec.weights) == 1


def test_weight_validator_error_in_generate(spark: SparkSession):
    spec = {'val': ChoiceSpec(values=[1, 2], weights=[0.9, 0.1])}

    synthetic = SingleFieldModel.generate_data(spark, specs=spec, n_rows=10)
    assert synthetic.where(F.col('val') == 1).count() > synthetic.where(F.col('val') == 2).count()


def test_use_field_alias(spark: SparkSession):
    data_gen = AliasModel.generate_data(spark, n_rows=1)

    assert data_gen.columns == ['_val']
