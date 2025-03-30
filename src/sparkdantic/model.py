import inspect
import sys
from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
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
from pydantic.fields import FieldInfo
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
    ) -> 'StructType':
        """Generates a PySpark schema from the model fields. This operates similarly to
        `pydantic.BaseModel.model_json_schema()`.

        Args:
            safe_casting (bool): Indicates whether to use safe casting for integer types.
            by_alias (bool): Indicates whether to use attribute aliases (`serialization_alias` or `alias`) or not.
            mode (pydantic.json_schema.JsonSchemaMode): The mode in which to generate the schema.

        Returns:
            pyspark.sql.types.StructType: The generated PySpark schema.
        """
        return create_spark_schema(cls, safe_casting, by_alias, mode)

    @classmethod
    def model_json_spark_schema(
        cls,
        safe_casting: bool = False,
        by_alias: bool = True,
        mode: JsonSchemaMode = 'validation',
    ) -> Dict[str, Any]:
        """Generates a PySpark JSON compatible schema from the model fields. This operates similarly to
        `pydantic.BaseModel.model_json_schema()`.

        Args:
            safe_casting (bool): Indicates whether to use safe casting for integer types.
            by_alias (bool): Indicates whether to use attribute aliases (`serialization_alias` or `alias`) or not.
            mode (pydantic.json_schema.JsonSchemaMode): The mode in which to generate the schema.

        Returns:
            Dict[str, Any]: The generated PySpark JSON schema.
        """
        return create_json_spark_schema(cls, safe_casting, by_alias, mode)


def create_json_spark_schema(
    model: Type[BaseModelOrSparkModel],
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: JsonSchemaMode = 'validation',
) -> Dict[str, Any]:
    """Generates a PySpark JSON compatible schema from the model fields. This operates similarly to
    `pydantic.BaseModel.model_json_schema()`.

    Args:
        model (pydantic.BaseModel or SparkModel): The pydantic model to generate the schema from.
        safe_casting (bool): Indicates whether to use safe casting for integer types.
        by_alias (bool): Indicates whether to use attribute aliases (`serialization_alias` or `alias`) or not.
        mode (pydantic.json_schema.JsonSchemaMode): The mode in which to generate the schema.

    Returns:
        Dict[str, Any]: The generated PySpark JSON schema
    """
    if not _is_base_model(model):
        raise TypeError('`model` must be of type `SparkModel` or `pydantic.BaseModel`')

    if mode not in get_args(JsonSchemaMode):
        raise ValueError(f'`mode` must be one of {get_args(JsonSchemaMode)}')

    fields = []
    for name, info in model.model_fields.items():
        if by_alias:
            name = _get_field_alias(name, info, mode)

        field_info_extra = info.json_schema_extra or {}
        override = field_info_extra.get('spark_type')
        field_type = _get_union_type_arg(info.annotation)

        spark_type: Union[str, Dict[str, Any]]

        try:
            if _is_base_model(field_type):
                spark_type = create_json_spark_schema(field_type, safe_casting, by_alias, mode)
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
                spark_type = _from_python_type(field_type, info.metadata, safe_casting, by_alias)
        except Exception as raised_error:
            raise TypeConversionError(
                f'Error converting field `{name}` to PySpark type'
            ) from raised_error

        nullable = _is_optional(info.annotation)
        struct_field = {
            'name': name,
            'type': spark_type,
            'nullable': nullable,
            'metadata': {},
        }
        fields.append(struct_field)
    return {
        'type': 'struct',
        'fields': fields,
    }


def create_spark_schema(
    model: Type[BaseModelOrSparkModel],
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: JsonSchemaMode = 'validation',
) -> 'StructType':
    """Generates a PySpark schema from the model fields.

    Args:
        model (pydantic.BaseModel or SparkModel): The pydantic model to generate the schema from.
        safe_casting (bool): Indicates whether to use safe casting for integer types.
        by_alias (bool): Indicates whether to use attribute aliases (`serialization_alias` or `alias`) or not.
        mode (pydantic.json_schema.JsonSchemaMode): The mode in which to generate the schema.

    Returns:
        pyspark.sql.types.StructType: The generated PySpark schema.
    """
    utils.require_pyspark()
    json_schema = create_json_spark_schema(model, safe_casting, by_alias, mode)
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
        return create_json_spark_schema(py_type, safe_casting, by_alias, 'validation')

    args = get_args(py_type)
    origin = get_origin(py_type)

    if origin is None and py_type in (list, dict):
        raise TypeError(f'Type argument(s) missing from {py_type.__name__}')

    # Convert complex types
    if origin is list:
        element_type = _from_python_type(args[0], [], safe_casting, by_alias)
        contains_null = _is_optional(args[0])
        return {
            'type': 'array',
            'elementType': element_type,
            'containsNull': contains_null,
        }

    if origin is dict:
        key_type = _from_python_type(args[0], [], safe_casting, by_alias)
        value_type = _from_python_type(args[1], [], safe_casting, by_alias)
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


def _get_union_type_arg(t: Type) -> Type:
    """Returns the inner type from a Union or the type itself if it's not a Union.

    Args:
        t (Type): The Union type to get the inner type from.

    Returns:
        Type: The inner type or the original type.
    """
    return get_args(t)[0] if get_origin(t) in (Union, UnionType) else t


def _get_field_alias(name: str, field_info: FieldInfo, mode: JsonSchemaMode = 'validation') -> str:
    """Returns the field's alias (if defined) or name based on the mode.
    When both alias and serialization_alias are used, serialization_alias takes precedence.
    Similarly, when both alias and validation_alias are used, validation_alias takes precedence.

    Args:
        name (str): The model field name.
        field_info (pydantic.FieldInfo): The model field info from which to get the alias or name.
        mode (pydantic.json_schema.JsonSchemaMode): The mode in which to generate the schema.

    Returns:
        str: The field alias or name.
    """
    if mode == 'serialization':
        alias = field_info.serialization_alias or field_info.alias
    elif mode == 'validation':
        validation_alias = field_info.validation_alias
        if validation_alias is None or isinstance(validation_alias, AliasPath):
            alias = field_info.alias
        elif isinstance(validation_alias, AliasChoices):
            alias = validation_alias.choices[0]
        else:
            alias = validation_alias
    return alias or name


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
