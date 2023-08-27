from typing import Dict, List, Optional, Union, get_args, get_origin

import dbldatagen as dg
from faker import Faker
from pyspark.sql import SparkSession

from sparkdantic import ColumnGenerationSpec, SparkModel


class SimpleModel(SparkModel):
    name: str
    age: Optional[int] = None
    jobs: List[str]
    map: Dict[str, str]


def test_generate_data(spark: SparkSession):
    data_gen = SimpleModel.generate_data(spark, n_rows=10).build().collect()
    assert data_gen is not None
    assert len(data_gen) == 10


def test_weights_conversion(spark: SparkSession):
    n_rows = 1000
    generator = dg.DataGenerator(spark, rows=n_rows)
    weights = [0.1, 0.2, 0.7]
    adjusted = SimpleModel._spec_weights_to_row_count(generator, weights)
    assert generator.rowCount == n_rows
    assert adjusted == [100, 200, 700]
    assert sum(adjusted) == n_rows


def _check_types_and_subtypes_match(field, row, name):
    container = get_origin(field.annotation)
    if container:
        inner = get_args(field.annotation)
        if container is not Union:
            if container is not dict:
                assert isinstance(row[name], container)
        else:
            assert isinstance(row[name], inner[0])
    else:
        assert isinstance(row[name], field.annotation)


def test_column_defaults(spark: SparkSession):
    data_gen = SimpleModel.generate_data(spark, n_rows=1000).build().collect()
    for row in data_gen:
        for name, field in SimpleModel.model_fields.items():
            _check_types_and_subtypes_match(field, row, name)


def test_columns_with_specs(spark: SparkSession, faker: Faker):
    n_rows = 1000
    names = [faker.name() for _ in range(100)]
    specs = {
        'name': ColumnGenerationSpec(values=names),
        'age': ColumnGenerationSpec(min_value=10, max_value=20),
        'jobs': ColumnGenerationSpec(values=[faker.job(), faker.job()]),
    }
    data_gen = SimpleModel.generate_data(spark, specs=specs, n_rows=n_rows).build().collect()
    for row in data_gen:
        assert row.name in names
        assert row.age >= 10
        assert row.age <= 20
        for name, field in SimpleModel.model_fields.items():
            _check_types_and_subtypes_match(field, row, name)
