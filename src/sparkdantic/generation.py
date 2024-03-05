from abc import ABC, abstractmethod
from random import choices
from typing import Any, Callable, ClassVar, List, Optional

from faker import Faker


class GenerationSpec(ABC):
    faker: ClassVar[Faker] = Faker()

    @abstractmethod
    def value(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)


class RangeSpec(GenerationSpec):
    def __init__(self, min_val: int, max_val: int, nullable: bool = False, null_prob: float = 0.1):
        self.min = min_val
        self.max = max_val
        self.nullable = nullable
        self.null_prob = null_prob

    def value(self) -> Any:
        if self.nullable and self.faker.random.random() < self.null_prob:
            return None
        return self.faker.random_int(min=self.min, max=self.max)


class ChoiceSpec(GenerationSpec):
    def __init__(self, values: List[Any], weights: Optional[List[float]] = None):
        self.values = values
        self.weights = weights

    def value(self, *args, **kwargs):
        return choices(self.values, weights=self.weights, k=1)[0]


class FuncSpec(GenerationSpec):
    def __init__(self, func: Callable, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def value(self, *args, **kwargs):
        return self.func(*self.args, **self.kwargs)


class ValueSpec(GenerationSpec):
    def __init__(self, value: Any):
        self._value = value

    def value(self, *args, **kwargs):
        return self._value
