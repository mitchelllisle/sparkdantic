from typing import Any, Dict, Iterable, List, Optional, Union

from pydantic import BaseModel, Field, field_validator


class ColumnGenerationSpec(BaseModel):
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
    @classmethod
    def weights_validator(cls, v: List[float]) -> List[float]:
        summed = sum(v)
        if summed != 1:
            raise ValueError(f'weights must sum up to 1 for ColumnGenerationSpec not {summed}')
        return v
