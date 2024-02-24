from abc import ABC, abstractmethod
from random import choices
from typing import Any, ClassVar, List, Optional

from faker import Faker


class GenerationSpec(ABC):
    faker: ClassVar[Faker] = Faker()

    @abstractmethod
    def value(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)


class RangeSpec(GenerationSpec):
    def __init__(self, min_val: int, max_val: int):
        self.min = min_val
        self.max = max_val

    def value(self) -> Any:
        return self.faker.random_int(min=self.min, max=self.max)


class ChoiceSpec(GenerationSpec):
    def __init__(self, values: List[Any], weights: Optional[List[float]] = None):
        self.values = values
        self.weights = weights

    def value(self, *args, **kwargs):
        return choices(self.values, weights=self.weights, k=1)[0]
