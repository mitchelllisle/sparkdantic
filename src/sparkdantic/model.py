import inspect
import sys
from collections.abc import Iterable
from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from itertools import chain
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Dict,
    Literal,
    Optional,
    Type,
    Union,
    get_args,
    get_origin,
)
from uuid import UUID

from pydantic import AliasChoices, AliasPath, BaseModel, ConfigDict, Field, SecretBytes, SecretStr
from pydantic.fields import ComputedFieldInfo, FieldInfo
from pydantic.json_schema import JsonSchemaMode

from sparkdantic import utils
from sparkdantic.exceptions import TypeConversionError

if utils.have_pyspark:
    utils.require_pyspark_version_in_range()
    from pyspark.sql.types import DataType, StructType

if TYPE_CHECKING:
    from pyspark.sql.types import DataType, StructType

if sys.version_info > (3, 10):
    from types import UnionType  # pragma: no cover
else:
    UnionType = Union  # pragma: no cover

if sys.version_info > (3, 11):
    from enum import EnumType  # pragma: no cover
else:
    EnumType = Type[Enum]  # pragma: no cover

MixinType = Union[Type[int], Type[str]]

FieldInfoUnion = Union[FieldInfo, ComputedFieldInfo]

BaseModelOrSparkModel = Union[BaseModel, 'SparkModel']

_type_mapping = MappingProxyType(
    {
        int: 'integer',
        float: 'double',
        str: 'string',
        SecretStr: 'string',
        bool: 'boolean',
        bytes: 'binary',
        SecretBytes: 'binary',
        list: 'array',
        dict: 'map',
        datetime: 'timestamp',
        date: 'date',
        Decimal: 'decimal',
        timedelta: 'interval day to second',
        UUID: 'string',
    }
)


def SparkField(*args, spark_type: Optional[Union[Type['DataType'], str]] = None, **kwargs) -> Field:
    if spark_type is not None:
        kwargs['spark_type'] = spark_type
    return Field(*args, **kwargs)


class SparkModel(BaseModel):
    """A Pydantic BaseModel subclass with model to PySpark schema conversion.

    Methods:
        model_spark_schema: Generates a PySpark schema from the model fields.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)

    @classmethod
    def model_spark_schema(
        cls,
        safe_casting: bool = False,
        by_alias: bool = True,
        mode: JsonSchemaMode = 'validation',
        exclude_fields: bool = False,
    ) -> 'StructType':
        """Generates a PySpark schema from the model fields. This operates similarly to
        `pydantic.BaseModel.model_json_schema()`.

        Args:
            safe_casting (bool): Indicates whether to use safe casting for integer types.
            by_alias (bool): Indicates whether to use attribute aliases (`serialization_alias` or `alias`) or not.
            mode (pydantic.json_schema.JsonSchemaMode): The mode in which to generate the schema.
            exclude_fields (bool): Indicates whether to exclude fields from the schema. Fields to be excluded should
              be annotated with `Field(exclude=True)` field attribute

        Returns:
            pyspark.sql.types.StructType: The generated PySpark schema.
        """
        return create_spark_schema(cls, safe_casting, by_alias, mode, exclude_fields)

    @classmethod
    def model_json_spark_schema(
        cls,
        safe_casting: bool = False,
        by_alias: bool = True,
        mode: JsonSchemaMode = 'validation',
        exclude_fields: bool = False,
    ) -> Dict[str, Any]:
        """Generates a PySpark JSON compatible schema from the model fields. This operates similarly to
        `pydantic.BaseModel.model_json_schema()`.

        Args:
            safe_casting (bool): Indicates whether to use safe casting for integer types.
            by_alias (bool): Indicates whether to use attribute aliases (`serialization_alias` or `alias`) or not.
            mode (pydantic.json_schema.JsonSchemaMode): The mode in which to generate the schema.
            exclude_fields (bool): Indicates whether to exclude fields from the schema. Fields to be excluded should
              be annotated with `Field(exclude=True)` field attribute

        Returns:
            Dict[str, Any]: The generated PySpark JSON schema.
        """
        return create_json_spark_schema(cls, safe_casting, by_alias, mode, exclude_fields)

    @classmethod
    def model_ddl_spark_schema(
        cls,
        safe_casting: bool = False,
        by_alias: bool = True,
        mode: JsonSchemaMode = 'validation',
        exclude_fields: bool = False,
    ) -> str:
        """Generates a Pyspark schema DDL String from the model fields.

        Args:
            model (pydantic.BaseModel or SparkModel): The pydantic model to generate the schema from.
            safe_casting (bool): Indicates whether to use safe casting for integer types.
            by_alias (bool): Indicates whether to use attribute aliases (`serialization_alias` or `alias`) or not.
            mode (pydantic.json_schema.JsonSchemaMode): The mode in which to generate the schema.
            exclude_fields (bool): Indicates whether to exclude fields from the schema. Fields to be excluded should
              be annotated with `Field(exclude=True)` field attribute

        Returns:
            str: The generated DDL PySpark schema.
        """
        return create_ddl_spark_schema(cls, safe_casting, by_alias, mode, exclude_fields)


def create_json_spark_schema(
    model: Type[BaseModelOrSparkModel],
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: JsonSchemaMode = 'validation',
    exclude_fields: bool = False,
) -> Dict[str, Any]:
    """Generates a PySpark JSON compatible schema from the model fields. This operates similarly to
    `pydantic.BaseModel.model_json_schema()`.

    Args:
        model (pydantic.BaseModel or SparkModel): The pydantic model to generate the schema from.
        safe_casting (bool): Indicates whether to use safe casting for integer types.
        by_alias (bool): Indicates whether to use attribute aliases (`serialization_alias` or `alias`) or not.
        mode (pydantic.json_schema.JsonSchemaMode): The mode in which to generate the schema.
        exclude_fields (bool): Indicates whether to exclude fields from the schema. Fields to be excluded should
          be annotated with `Field(exclude=True)` field attribute

    Returns:
        Dict[str, Any]: The generated PySpark JSON schema
    """
    if not _is_base_model(model):
        raise TypeError('`model` must be of type `SparkModel` or `pydantic.BaseModel`')

    if mode not in get_args(JsonSchemaMode):
        raise ValueError(f'`mode` must be one of {get_args(JsonSchemaMode)}')

    fields = []
    for name, info in _get_schema_items(model, mode):
        if exclude_fields and getattr(info, 'exclude', False):
            continue
        if by_alias:
            name = _get_field_alias(name, info, mode)

        field_info_extra = info.json_schema_extra or {}
        override = field_info_extra.get('spark_type')
        annotation_or_return_type = _get_annotation_or_return_type(info)
        field_type = _get_union_type_arg(annotation_or_return_type)

        description = getattr(info, 'description', None)
        comment = {'comment': description} if description else {}

        spark_type: Union[str, Dict[str, Any]]

        try:
            if _is_base_model(field_type):
                spark_type = create_json_spark_schema(
                    field_type, safe_casting, by_alias, mode, exclude_fields
                )
            elif override is not None:
                if isinstance(override, str):
                    spark_type = override
                elif utils.have_pyspark and _is_spark_datatype(override):
                    spark_type = override.typeName()
                    if spark_type == 'struct':
                        spark_type = override.jsonValue()
                else:
                    msg = '`spark_type` override should be a `str` type name (e.g. long)'
                    if utils.have_pyspark:
                        msg += ' or `pyspark.sql.types.DataType` (e.g. LongType)'
                    msg += f', but got {override}'
                    raise TypeError(msg)
            elif isinstance(field_type, str):
                spark_type = field_type
            elif utils.have_pyspark and _is_spark_datatype(field_type):
                spark_type = field_type.typeName()
            else:
                metadata = _get_metadata(info)
                spark_type = _from_python_type(
                    field_type, metadata, safe_casting, by_alias, mode, exclude_fields
                )
        except Exception as raised_error:
            raise TypeConversionError(
                f'Error converting field `{name}` to PySpark type'
            ) from raised_error

        nullable = _is_optional(annotation_or_return_type)
        struct_field: Dict[str, Any] = {
            'name': name,
            'type': spark_type,
            'nullable': nullable,
            'metadata': comment,
        }
        fields.append(struct_field)
    return {
        'type': 'struct',
        'fields': fields,
    }


def create_ddl_spark_schema(
    model: Type[BaseModelOrSparkModel],
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: JsonSchemaMode = 'validation',
    exclude_fields: bool = False,
) -> str:
    """Generates a Pyspark schema DDL String from the model fields.

    Args:
        model (pydantic.BaseModel or SparkModel): The pydantic model to generate the schema from.
        safe_casting (bool): Indicates whether to use safe casting for integer types.
        by_alias (bool): Indicates whether to use attribute aliases (`serialization_alias` or `alias`) or not.
        mode (pydantic.json_schema.JsonSchemaMode): The mode in which to generate the schema.
        exclude_fields (bool): Indicates whether to exclude fields from the schema. Fields to be excluded should
          be annotated with `Field(exclude=True)` field attribute

    Returns:
        str: The generated DDL PySpark schema.
    """
    utils.require_pyspark()
    json_schema = create_json_spark_schema(model, safe_casting, by_alias, mode, exclude_fields)
    return json_schema_to_ddl(json_schema)


def create_spark_schema(
    model: Type[BaseModelOrSparkModel],
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: JsonSchemaMode = 'validation',
    exclude_fields: bool = False,
) -> 'StructType':
    """Generates a PySpark schema from the model fields.

    Args:
        model (pydantic.BaseModel or SparkModel): The pydantic model to generate the schema from.
        safe_casting (bool): Indicates whether to use safe casting for integer types.
        by_alias (bool): Indicates whether to use attribute aliases (`serialization_alias` or `alias`) or not.
        mode (pydantic.json_schema.JsonSchemaMode): The mode in which to generate the schema.
        exclude_fields (bool): Indicates whether to exclude fields from the schema. Fields to be excluded should
          be annotated with `Field(exclude=True)` field attribute

    Returns:
        pyspark.sql.types.StructType: The generated PySpark schema.
    """
    utils.require_pyspark()
    json_schema = create_json_spark_schema(model, safe_casting, by_alias, mode, exclude_fields)
    return StructType.fromJson(json_schema)


def _get_spark_type(t: Type) -> str:
    """Returns the corresponding PySpark data type for a given Python type.

    Args:
        t (Type): The Python type to convert to a PySpark data type.

    Returns:
        DataType: The corresponding PySpark data type.

    Raises:
        TypeError: If the type is not recognized in the type map.
    """
    spark_type = _type_mapping.get(t)
    if spark_type is None:
        raise TypeError(f'Type {t} not recognized')
    return spark_type


def _get_enum_mixin_type(t: EnumType) -> MixinType:
    """Returns the mixin type of an Enum.

    Args:
        t (EnumType): The Enum to get the mixin type from.

    Returns:
        MixinType: The type mixed with the Enum.

    Raises:
        TypeError: If the mixin type is not supported (int and str are supported).
    """
    if issubclass(t, int):
        return int
    elif issubclass(t, str):
        return str
    else:
        raise TypeError(f'Enum {t} is not supported. Only int and str mixins are supported.')


def _from_python_type(
    type_: Type,
    metadata: list[Any],
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: JsonSchemaMode = 'validation',
    exclude_fields: bool = False,
) -> Union[str, Dict[str, Any]]:
    """Converts a Python type to a corresponding PySpark data type.

    Args:
        type_ (Type): The python type to convert to a PySpark data type.
        metadata (list): The metadata for the field.
        safe_casting (bool): Indicates whether to use safe casting for integer types.
        by_alias (bool): Indicates whether to use attribute aliases or not.

    Returns:
        Union[str, Dict[str, Any]]: The corresponding PySpark data type (dict for complex types).
    """
    py_type = _get_union_type_arg(type_)

    if _is_base_model(py_type):
        return create_json_spark_schema(py_type, safe_casting, by_alias, mode, exclude_fields)

    args = get_args(py_type)
    origin = get_origin(py_type)

    if origin is None and py_type in (list, dict):
        raise TypeError(f'Type argument(s) missing from {py_type.__name__}')

    # Convert complex types
    if origin is list:
        element_type = _from_python_type(args[0], [], safe_casting, by_alias, mode, exclude_fields)
        contains_null = _is_optional(args[0])
        return {
            'type': 'array',
            'elementType': element_type,
            'containsNull': contains_null,
        }

    if origin is dict:
        key_type = _from_python_type(args[0], [], safe_casting, by_alias, mode, exclude_fields)
        value_type = _from_python_type(args[1], [], safe_casting, by_alias, mode, exclude_fields)
        value_contains_null = _is_optional(args[1])
        return {
            'type': 'map',
            'keyType': key_type,
            'valueType': value_type,
            'valueContainsNull': value_contains_null,
        }

    if origin is Literal:
        # PySpark doesn't have an equivalent type for Literal. To allow Literal usage with a model we check all the
        # types of values within Literal. If they are all the same, use that type as our new type.
        literal_arg_types = set(map(lambda a: type(a), args))
        if len(literal_arg_types) > 1:
            raise TypeError(
                'Multiple types detected in `Literal` type. Only one consistent arg type is supported.'
            )
        py_type = literal_arg_types.pop()

    if origin is Annotated:
        # first arg of annotated type is the type, second is metadata that we don't do anything with (yet)
        py_type = args[0]

    if issubclass(py_type, Enum):
        py_type = _get_enum_mixin_type(py_type)

    spark_type = _get_spark_type(py_type)

    if safe_casting is True and spark_type == 'integer':
        spark_type = 'long'

    if spark_type == 'decimal':
        meta = None if len(metadata) < 1 else deepcopy(metadata).pop()
        max_digits = getattr(meta, 'max_digits', 10)
        decimal_places = getattr(meta, 'decimal_places', 0)
        return f'decimal({max_digits}, {decimal_places})'

    return spark_type


def _is_base_model(cls: Type) -> bool:
    """Checks if a class is a pydantic.BaseModel.

    Args:
        cls: The class to check.

    Returns:
        bool: True if it is a subclass, otherwise False.
    """
    try:
        return inspect.isclass(cls) and issubclass(cls, BaseModel)
    except TypeError:
        return False


def _is_optional(t: Type) -> bool:
    """Determines if a type is optional.

    Args:
        t (Type): The Python type to check for nullability.

    Returns:
        bool: a boolean indicating nullability.
    """
    return get_origin(t) in (Union, UnionType) and type(None) in get_args(t)


def _get_schema_items(
    model: BaseModelOrSparkModel, mode: JsonSchemaMode
) -> Iterable[tuple[str, FieldInfoUnion]]:
    """Returns the appropriate fields based on the schema mode.

    Args:
        model (BaseModelOrSparkModel): The model to get fields from.
        mode (JsonSchemaMode): The schema generation mode.

    Returns:
        Iterable[tuple[str, FieldInfoUnion]]: Iterator of field items.
    """
    if mode == 'serialization':
        return chain(model.model_fields.items(), model.model_computed_fields.items())

    return chain(model.model_fields.items())


def _get_union_type_arg(t: Type) -> Type:
    """Returns the inner type from a Union or the type itself if it's not a Union.

    Args:
        t (Type): The Union type to get the inner type from.

    Returns:
        Type: The inner type or the original type.
    """
    return get_args(t)[0] if get_origin(t) in (Union, UnionType) else t


def _get_annotation_or_return_type(field_info: FieldInfoUnion) -> Type:
    """Returns the annotation or return type of a field.

    Args:
        field_info (FieldInfoUnion): The model field info from which to get the annotation or return type.
            Can be either a regular FieldInfo or ComputedFieldInfo.

    Returns:
        Type: The annotation type for regular fields or return type for computed fields.
    """
    return field_info.annotation if isinstance(field_info, FieldInfo) else field_info.return_type


def _get_field_alias(
    name: str, field_info: FieldInfoUnion, mode: JsonSchemaMode = 'validation'
) -> str:
    """Returns the field's alias (if defined) or name based on the mode.

    Args:
        name (str): The model field name.
        field_info (FieldInfoUnion): The model field info from which to get the alias or name.
        mode (JsonSchemaMode): The mode in which to generate the schema. Defaults to 'validation'.

    Returns:
        str: The field alias if defined, otherwise the original field name.
    """
    if mode == 'serialization':
        alias = _get_alias_by_attr(field_info, 'serialization_alias')
    elif mode == 'validation':
        alias = _get_alias_by_attr(field_info, 'validation_alias')
    return alias or name


def _get_alias_by_attr(field_info: FieldInfoUnion, attr: str) -> Optional[str]:
    """Gets the alias value for a specific attribute from a field info object.

    Args:
        field_info (FieldInfoUnion): The field info object to extract the alias from.
        attr (str): The attribute name to look for (e.g., 'serialization_alias', 'validation_alias').

    Returns:
        Optional[str]: The resolved alias string if found, None otherwise.
    """
    if hasattr(field_info, attr):
        alias = getattr(field_info, attr)
    else:
        alias = getattr(field_info, 'alias', None)

    if alias is None or isinstance(alias, AliasPath):
        return getattr(field_info, 'alias')
    elif isinstance(alias, AliasChoices):
        return alias.choices[0]

    return alias


def _get_metadata(field_info: FieldInfoUnion) -> list[Any]:
    """Returns the metadata of a field.

    Args:
        field_info (FieldInfoUnion): The model field info from which to get the metadata.
            Can be either a regular FieldInfo or ComputedFieldInfo.

    Returns:
        list[Any]: The metadata list for regular fields, empty list for computed fields.
    """
    return field_info.metadata if isinstance(field_info, FieldInfo) else []


def _is_spark_datatype(t: Type) -> bool:
    """Determines if a type is a PySpark DataType.

    Args:
        t (Type): The Python type to check.

    Returns:
        bool: a boolean indicating if the type is a PySpark DataType.
    """
    if isinstance(t, StructType):
        return True
    return inspect.isclass(t) and issubclass(t, DataType)


def _json_type_to_ddl(json_type: Union[str, Dict[str, Any]]) -> str:
    """Maps JSON schema types to DDL types.

    Args:
        json_type (Union[str, Dict[str, Any]]): The JSON schema type to convert.

    Returns:
        str: The DDL type representation.
    """
    if isinstance(json_type, str):
        # Map INTEGER to INT
        if json_type.upper() == 'INTEGER':
            return 'INT'
        elif json_type.upper().startswith('DECIMAL'):
            return json_type.upper().replace(' ', '')  # Remove whitespaces
        else:
            return json_type.upper()

    if json_type['type'] == 'struct':
        nested_fields = [
            f"{field['name']}: {_json_type_to_ddl(field['type'])}" for field in json_type['fields']
        ]
        return f"STRUCT<{', '.join(nested_fields)}>"

    elif json_type['type'] == 'array':
        element_type = _json_type_to_ddl(json_type['elementType'])
        return f'ARRAY<{element_type}>'

    elif json_type['type'] == 'map':
        key_type = _json_type_to_ddl(json_type['keyType'])
        value_type = _json_type_to_ddl(json_type['valueType'])
        return f'MAP<{key_type}, {value_type}>'
    else:
        raise TypeError(f"Unsupported JSON type: {json_type['type']}")


def json_schema_to_ddl(json_schema: Dict[str, Any]) -> str:
    """Converts a JSON schema to a DDL string representation matching Spark's format.

    Args:
        json_schema (Dict[str, Any]): The JSON schema to convert.

    Returns:
        str: The DDL string representation of the schema.
    """

    field_ddls = []
    for field in json_schema['fields']:
        field_ddl = f"{field['name']} {_json_type_to_ddl(field['type'])}"
        if not field['nullable']:
            field_ddl += ' NOT NULL'
        field_ddls.append(field_ddl)

    return ','.join(field_ddls)
