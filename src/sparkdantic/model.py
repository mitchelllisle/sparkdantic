from typing import Any, Dict

from pydantic import BaseModel
from pydantic.json_schema import CoreSchema, GenerateJsonSchema
from pyspark.sql import types as T


def _is_nullable(field_schema: Dict[str, Any]) -> bool:
    return field_schema.get('type') == 'null' or 'null' in field_schema.get('type', [])


class SparkSchemaGenerator(GenerateJsonSchema):
    def generate(self, schema: CoreSchema, mode='validation') -> Dict[str, Any]:
        base_schema = super().generate(schema, mode=mode)
        return {
            'type': 'struct',
            'fields': self._transform_fields(base_schema.get('properties', {})),
        }

    def _transform_fields(self, properties: Dict[str, Any]) -> list:
        fields = []
        for field_name, field_schema in properties.items():
            field_def: dict = {
                'name': field_name,
                'type': self._get_spark_type(field_schema),
                'nullable': _is_nullable(field_schema),
                'metadata': {},
            }
            fields.append(field_def)
        return fields

    def _get_spark_type(self, field_schema: Dict[str, Any]) -> str | Dict:
        type_mapping = {
            'string': 'string',
            'integer': 'integer',
            'number': 'double',
            'boolean': 'boolean',
            'array': 'array',
        }

        field_type = field_schema.get('type')
        if isinstance(field_type, str):
            return type_mapping.get(field_type, 'string')

        if field_type == 'object':
            return {
                'type': 'struct',
                'fields': self._transform_fields(field_schema.get('properties', {})),
            }

        return 'string'


class SparkModel(BaseModel):
    @classmethod
    def model_spark_schema(cls):
        return create_spark_schema(cls)


def create_spark_schema(model: BaseModel) -> T.StructType:
    schema = model.model_json_schema(schema_generator=SparkSchemaGenerator)
    return T.StructType.fromJson(schema)
