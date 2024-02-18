from typing import Any, Dict, Iterable, List, Optional, Union

from pydantic import BaseModel, Field, field_validator


class ColumnGenerationSpec(BaseModel):
    """
    Specifications for generating a DataFrame column.

    Attributes:
        min_value (Optional[float]): Min value for range. Use with `max_value` and `step` or use `data_range`.
        max_value (Optional[float]): Max value for range. Use with `min_value` and `step` or use `data_range`.
        step (Optional[float]): Step for range. Use with `min_value` and `max_value` or use `data_range`.
        prefix (Optional[str]): Prefix for column name.
        random (Optional[bool]): If True, generates random values. Defaults to False.
        random_seed_method (Optional[str]): 'fixed' uses a fixed seed, 'hash_fieldname' uses a hash of the field name.
        random_seed (Optional[int]): Seed for random numbers, behavior depends on `random_seed_method`.
        distribution (Optional[str]): Distribution of random values. Can be "normal".
        base_column (Optional[Union[str, List[str]]]): Base column(s) for controlling data generation.
        values (Optional[List[Any]]): List of discrete values for the column.
        weights (Optional[List[Union[float, int]]]): Weights for `values`, must sum to 1.
        percent_nulls (Optional[float]): Fraction for nulls between 0.0 and 1.0.
        unique_values (Optional[int]): Number of unique values. Alternate to `data_range`.
        begin (Optional[str]): Start of date range.
        end (Optional[str]): End of date range.
        interval (Optional[str]): Interval for date range.
        data_range (Optional[Union[str, dict]]): Use instead of `min_value`, `max_value`, and `step`.
        template (Optional[str]): String template for text generation.
        omit (Optional[bool]): If True, omit column from output.
        expr (Optional[str]): SQL expression for data generation.
        num_columns (Optional[int]): Number of columns for same spec.
        num_features (Optional[int]): Synonym for `num_columns`.
        struct_type (Optional[str]): If 'array', generates array value from multiple columns.
    """

    min_value: Optional[float] = Field(default=None, serialization_alias='minValue')
    max_value: Optional[float] = Field(default=None, serialization_alias='maxValue')
    step: Optional[float] = None
    prefix: Optional[str] = None
    random: Optional[bool] = None
    random_seed_method: Optional[str] = Field(default=None, serialization_alias='randomSeedMethod')
    random_seed: Optional[int] = Field(default=None, serialization_alias='randomSeed')
    distribution: Optional[str] = None
    base_column: Optional[Union[str, List[str]]] = Field(
        default=None, serialization_alias='baseColumn'
    )
    value: Optional[Any] = None
    values: Optional[List[Any]] = None
    dict_values: Optional[Dict[Any, Any]] = None
    weights: Optional[List[Union[float, int]]] = None
    percent_nulls: Optional[float] = Field(default=None, serialization_alias='percentNulls')
    unique_values: Optional[int] = Field(default=None, serialization_alias='uniqueValues')
    begin: Optional[str] = None
    end: Optional[str] = None
    interval: Optional[str] = None
    data_range: Optional[Union[str, dict]] = Field(default=None, serialization_alias='dataRange')
    template: Optional[str] = None
    omit: Optional[bool] = None
    expr: Optional[str] = None
    num_columns: Optional[int] = Field(default=None, serialization_alias='numColumns')
    num_features: Optional[int] = Field(default=None, serialization_alias='numFeatures')
    struct_type: Optional[str] = Field(default=None, serialization_alias='structType')
    mapping: Optional[Dict[Any, Any]] = Field(default=None)
    mapping_source: Optional[str] = Field(default=None)

    @field_validator('weights', mode='before')
    def weights_validator(cls, v: List[float]) -> List[float]:
        summed = sum(v)
        if summed != 1:
            raise ValueError(f'weights must sum up to 1 for ColumnGenerationSpec not {summed}')
        return v
