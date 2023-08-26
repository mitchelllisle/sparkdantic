from typing import Optional, get_args

from sparkdantic import ColumnGenerationSpec, SparkModel


class SimpleModel(SparkModel):
    name: str
    age: Optional[int] = None


def test_generate_data(spark):
    data_gen = SimpleModel.generate_data(spark, n_rows=10).build().collect()
    assert data_gen is not None
    assert len(data_gen) == 10


def test_column_defaults(spark):
    data_gen = SimpleModel.generate_data(spark, n_rows=10).build().collect()
    for row in data_gen:
        for name, field in SimpleModel.model_fields.items():
            types = get_args(field.annotation)
            if len(types) > 0:
                assert isinstance(row[name], types[0])
            else:
                assert isinstance(row[name], field.annotation)
