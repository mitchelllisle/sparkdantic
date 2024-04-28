from abc import ABC, abstractmethod
from random import choices
from typing import Any, Callable, ClassVar, List, Optional

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
    def __init__(self, min_val: int, max_val: int, *args, **kwargs):
        self.min = min_val
        self.max = max_val
        super().__init__(*args, **kwargs)

    def value(self) -> Any:
        if self.nullable and self.faker.random.random() < self.null_prob:
            return None
        return self.faker.random_int(min=self.min, max=self.max)


class ChoiceSpec(GenerationSpec):
    def __init__(self, values: List[Any], weights: Optional[List[float]] = None):
        self.values = values
        self.weights = weights
        super().__init__()

    def value(self, *args, **kwargs):
        return choices(self.values, weights=self.weights, k=1)[0]


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
