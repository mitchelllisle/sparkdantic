from enum import IntEnum
from typing import Dict, List, Optional

from pyspark.sql import SparkSession

from sparkdantic import ChoiceSpec, RangeSpec, SparkModel


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


def test_generator(spark: SparkSession):
    n_rows = 100
    data = SampleModel.generate_data(
        spark,
        specs={
            'logic_number': RangeSpec(min_val=0, max_val=100),
            'logic': ChoiceSpec(values=['a', 'b', 'c']),
        },
        n_rows=n_rows,
    )

    assert data.count() == n_rows
