import pytest

from sparkdantic.model import _json_type_to_ddl


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
