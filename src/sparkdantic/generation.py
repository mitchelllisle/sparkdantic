from abc import ABC, abstractmethod
from random import choices
from typing import Any, Callable, ClassVar, Dict, List, Optional

import pyspark.sql.types as T
from faker import Faker


class GenerationSpec(ABC):

    faker: ClassVar[Faker] = Faker()

    def __init__(self, nullable: bool = False, null_prob: float = 0.1):
        self.nullable = nullable
        self.null_prob = null_prob

    @abstractmethod
    def value(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)


class RangeSpec(GenerationSpec):
    def __init__(
        self, min_value: int, max_value: int, precision: Optional[int] = None, *args, **kwargs
    ):
        self.min = min_value
        self.max = max_value
        self.precision = precision
        super().__init__(*args, **kwargs)

    def value(self) -> Any:
        if self.nullable and self.faker.random.random() < self.null_prob:
            return None
        if self.precision:
            return self.faker.random.uniform(self.min, self.max)
        return self.faker.random_int(min=self.min, max=self.max)


class ChoiceSpec(GenerationSpec):
    def __init__(self, values: List[Any], n: int = 1, weights: Optional[List[float]] = None):
        self.values = values
        self.weights = weights
        self.n = n
        self._validate_weights()
        super().__init__()

    def _validate_weights(self):
        if self.weights:
            if sum(self.weights) != 1:
                raise ValueError(f'Weights must sum to 1, not {sum(self.weights)}')

    def value(self, *args, **kwargs):
        picked = choices(self.values, weights=self.weights, k=self.n)
        return picked[0] if self.n < 2 else picked


class FuncSpec(GenerationSpec):
    def __init__(self, func: Callable, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        super().__init__()

    def value(self, *args, **kwargs):
        return self.func(*self.args, **self.kwargs)


class ValueSpec(GenerationSpec):
    def __init__(self, value: Any):
        self._value = value
        super().__init__()

    def value(self, *args, **kwargs):
        return self._value


class MappingSpec(GenerationSpec):
    def __init__(self, mapping: Dict[Any, Any], mapping_source: str, default: Optional[Any] = None):
        self.mapping = mapping
        self.mapping_source = mapping_source
        self.default = default
        super().__init__()

    def value(self, *args, **kwargs):
        print(args, kwargs)
        return self.mapping_source


def default_generator(t: T.DataType) -> GenerationSpec:
    type_to_generator = {
        T.IntegerType(): RangeSpec(min_value=0, max_value=100),
        T.DoubleType(): RangeSpec(min_value=0, max_value=100, precision=2),
        T.StringType(): ChoiceSpec(values=['a', 'b', 'c']),
        T.BooleanType(): ChoiceSpec(values=[True, False]),
    }
    return type_to_generator.get(t, ValueSpec(value=None))  # type: ignore
