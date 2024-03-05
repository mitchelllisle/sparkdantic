from enum import IntEnum
from typing import Dict, List, Optional

from faker import Faker
from pyspark.sql import SparkSession

from sparkdantic import ChoiceSpec, FuncSpec, RangeSpec, SparkModel, ValueSpec


class IntTestEnum(IntEnum):
    X = 1
    Y = 2


class SingleFieldModel(SparkModel):
    val: int


class SampleModel(SparkModel):
    logic_number: int
    logic: str
    name: str
    age: Optional[int] = None
    jobs: List[str]
    map: Dict[str, str]
    single_value: int
    enum: IntTestEnum


def test_generator(spark: SparkSession, faker: Faker):
    n_rows = 100
    data = SampleModel.generate_data(
        spark,
        specs={
            'logic_number': RangeSpec(min_val=0, max_val=100),
            'logic': ChoiceSpec(values=['a', 'b', 'c']),
            'name': FuncSpec(func=faker.name),
            'age': RangeSpec(min_val=0, max_val=100, nullable=True),
            'single_value': ValueSpec(value=1),
        },
        n_rows=n_rows,
    )
    data.show()
    assert data.count() == n_rows
