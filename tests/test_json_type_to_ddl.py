import pytest
from pyspark.sql import types as spark_types

from sparkdantic.model import _json_type_to_ddl

WGS84_SRID = 4326
WEB_MERCATOR_SRID = 3857


def test_json_type_to_ddl_unknown_type():
    """
    Test the conversion of an unknown type to DDL.
    """
    # Define a mock JSON schema with an unknown type
    json_schema = {
        'type': 'unknown',
        'properties': {
            'field1': {'type': 'string'},
            'field2': {'type': 'integer'},
        },
    }

    # Call the function to convert JSON schema to DDL

    with pytest.raises(TypeError, match='Unsupported JSON type: unknown'):
        _json_type_to_ddl(json_schema)


@pytest.mark.parametrize(
    'json_type, expected_ddl',
    [
        ('long', 'BIGINT'),
        ('byte', 'TINYINT'),
        ('short', 'SMALLINT'),
        ('INTEGER', 'INT'),
        ('INT', 'INT'),
        ('bigint', 'BIGINT'),
        ('SMALLINT', 'SMALLINT'),
        ('TINYINT', 'TINYINT'),
        ('DECIMAL(10, 2)', 'DECIMAL(10,2)'),
    ],
)
def test_scalar_json_type_to_ddl(json_type, expected_ddl):
    assert _json_type_to_ddl(json_type) == expected_ddl


@pytest.mark.skipif(not hasattr(spark_types, 'GeometryType'), reason='Requires PySpark 4.1+')
@pytest.mark.parametrize(
    'json_type, expected_ddl',
    [
        ('GEOMETRY(OGC:CRS84)', f'GEOMETRY({WGS84_SRID})'),
        (f'geometry(EPSG:{WEB_MERCATOR_SRID})', f'GEOMETRY({WEB_MERCATOR_SRID})'),
        ('geometry(SRID:ANY)', 'GEOMETRY(ANY)'),
        ('GEOGRAPHY(OGC:CRS84, SPHERICAL)', f'GEOGRAPHY({WGS84_SRID})'),
        ('geography(SRID:ANY, SPHERICAL)', 'GEOGRAPHY(ANY)'),
    ],
)
def test_geospatial_json_type_to_ddl(json_type, expected_ddl, spark):
    actual_ddl = _json_type_to_ddl(json_type)
    assert spark_types.DataType.fromDDL(actual_ddl) == spark_types.DataType.fromDDL(expected_ddl)
