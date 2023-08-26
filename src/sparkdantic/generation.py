from typing import Any, List, Optional, Union

from pydantic import BaseModel, Field


class ColumnGenerationSpec(BaseModel):
    min_value: Optional[float] = Field(alias='minValue')
    max_value: Optional[float] = Field(alias='maxValue')
    step: Optional[float]
    prefix: Optional[str]
    random: Optional[bool]
    random_seed_method: Optional[str] = Field(alias='randomSeedMethod')
    random_seed: Optional[int] = Field(alias='randomSeed')
    distribution: Optional[str]
    base_column: Optional[Union[str, List[str]]] = Field(alias='baseColumn')
    values: Optional[List[Any]]
    weights: Optional[List[float]]
    percent_nulls: Optional[float] = Field(alias='percentNulls')
    unique_values: Optional[int] = Field(alias='uniqueValues')
    begin: Optional[str]
    end: Optional[str]
    interval: Optional[str]
    data_range: Optional[Union[str, dict]] = Field(alias='dataRange')
    template: Optional[str]
    omit: Optional[bool]
    expr: Optional[str]
    num_columns: Optional[int] = Field(alias='numColumns')
    num_features: Optional[int] = Field(alias='numFeatures')
    struct_type: Optional[str] = Field(alias='structType')
