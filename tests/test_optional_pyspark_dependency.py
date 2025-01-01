import pydantic
import pytest

from sparkdantic import model, utils
from sparkdantic.exceptions import SparkdanticImportError


@pytest.fixture
def no_pyspark(monkeypatch):
    for module in [utils, model]:
        monkeypatch.setattr(module, 'have_pyspark', False)
        monkeypatch.setattr(module, 'pyspark_import_error', ImportError('No module named pyspark'))


def test_create_spark_schema_raises_import_error_when_no_pyspark(no_pyspark):
    class NoPySparkModel(pydantic.BaseModel):
        pass

    with pytest.raises(ImportError) as exc:
        model.create_spark_schema(NoPySparkModel)
    assert 'No module named pyspark' == str(exc.value)


def test_model_spark_schema_raises_import_error_when_no_pyspark(no_pyspark):
    class NoPySparkModel(model.SparkModel):
        pass

    with pytest.raises(ImportError) as exc:
        NoPySparkModel.model_spark_schema()
    assert 'No module named pyspark' == str(exc.value)


def test_no_pyspark_raises_import_error(no_pyspark):
    with pytest.raises(SparkdanticImportError) as exc:
        utils.require_pyspark_version_in_range()
    assert (
        'Pyspark is not installed. Install pyspark using `pip install sparkdantic[pyspark]`'
        == str(exc.value)
    )


@pytest.mark.parametrize(
    'version, raises_error',
    [
        ('2.4.0', True),
        ('3.3.0', False),
        ('3.5.0', False),
        ('4.0.0', True),
    ],
)
def test_require_pyspark_version_in_range(
    version,
    raises_error,
    monkeypatch,
):
    monkeypatch.setattr(utils, 'have_pyspark', True)
    monkeypatch.setattr(utils.pyspark, '__version__', version)

    if raises_error:
        with pytest.raises(SparkdanticImportError):
            utils.require_pyspark_version_in_range()

        assert f'Pyspark version >={utils.MIN_PYSPARK_VERSION},<{utils.MAX_PYSPARK_VERSION} is required, but found {version}'
    else:
        utils.require_pyspark_version_in_range()
