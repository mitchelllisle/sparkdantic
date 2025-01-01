import pydantic
import pytest

from sparkdantic import model


@pytest.fixture(autouse=True)
def no_pyspark(monkeypatch):
    monkeypatch.setattr(model, '_have_pyspark', False)
    monkeypatch.setattr(model, '_pyspark_import_error', ImportError('No module named pyspark'))


def test_create_spark_schema_raises_import_error_when_no_pyspark():
    class NoPySparkModel(pydantic.BaseModel):
        pass

    with pytest.raises(ImportError) as exc:
        model.create_spark_schema(NoPySparkModel)
    assert 'No module named pyspark' == str(exc.value)


def test_model_spark_schema_raises_import_error_when_no_pyspark():
    class NoPySparkModel(model.SparkModel):
        pass

    with pytest.raises(ImportError) as exc:
        NoPySparkModel.model_spark_schema()
    assert 'No module named pyspark' == str(exc.value)
