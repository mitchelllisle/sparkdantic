from pyspark.sql.types import StringType, StructField, StructType

from sparkdantic import SparkField, SparkModel


class DescriptionModel(SparkModel):
    field_with_description: str = SparkField(description='This is a test description.')
    field_without_description: str = SparkField()


def test_spark_schema_contains_field_descriptions():
    expected_schema = StructType(
        [
            StructField(
                'field_with_description',
                StringType(),
                False,
                metadata={'comment': 'This is a test description.'},
            ),
            StructField(
                'field_without_description',
                StringType(),
                False,
                metadata={},
            ),
        ]
    )

    actual_schema = DescriptionModel.model_spark_schema()
    assert actual_schema == expected_schema


def test_spark_schema_json_contains_field_descriptions():
    expected_json_schema = {
        'type': 'struct',
        'fields': [
            {
                'name': 'field_with_description',
                'type': 'string',
                'nullable': False,
                'metadata': {'comment': 'This is a test description.'},
            },
            {
                'name': 'field_without_description',
                'type': 'string',
                'nullable': False,
                'metadata': {},
            },
        ],
    }

    actual_json_schema = DescriptionModel.model_json_spark_schema()
    assert actual_json_schema == expected_json_schema
