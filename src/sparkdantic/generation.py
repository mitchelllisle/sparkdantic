from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from random import choices
from typing import Any, Callable, ClassVar, Dict, List, Optional, Union

import pyspark.sql.types as T
from faker import Faker


class GenerationSpec(ABC):
    """
    Abstract base class for generation specifications.

    Attributes:
        faker (ClassVar[Faker]): A class variable that generates fake data.
    """

    faker: ClassVar[Faker] = Faker()

    def __init__(self, nullable: bool = False, null_prob: float = 0.1):
        """
        Initialize GenerationSpec.

        Args:
            nullable (bool, optional): A flag indicating if the generated value can be null. Defaults to False.
            null_prob (float, optional): The probability of generating a null value if nullable is True. Defaults to 0.1
        """
        self.nullable = nullable
        self.null_prob = null_prob

    @abstractmethod
    def value(self, *args, **kwargs):
        """
        Abstract method to generate a value. Must be implemented by subclasses.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            The generated value.
        """
        pass

    def __call__(self, *args, **kwargs):
        """
        Make the instance callable. Delegates to the `value` method.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            The result of the `value` method.
        """
        return self.value(*args, **kwargs)


class RangeSpec(GenerationSpec):
    """
    Class for generating a range of values.

    Inherits from:
        GenerationSpec: Abstract base class for generation specifications.

    Attributes:
        min (int): The minimum value of the range.
        max (int): The maximum value of the range.
        precision (Optional[int]): The precision of the generated value if it's a float. Defaults to None.
    """

    def __init__(
        self, min_value: int, max_value: int, precision: Optional[int] = None, *args, **kwargs
    ):
        """
        Initialize RangeSpec.

        Args:
            min_value (int): The minimum value of the range.
            max_value (int): The maximum value of the range.
            precision (Optional[int], optional): The precision of the generated value if it's a float. Defaults to None.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        self.min = min_value
        self.max = max_value
        self.precision = precision
        super().__init__(*args, **kwargs)

    def value(self) -> Any:
        """
        Generate a value within the specified range.

        If the instance is nullable and a random number is less than the null probability, return None.
        If a precision is specified, return a random float within the range with the specified precision.
        Otherwise, return a random integer within the range.

        Returns:
            Any: The generated value.
        """
        if self.nullable and self.faker.random.random() < self.null_prob:
            return None
        if self.precision:
            return self.faker.random.uniform(self.min, self.max)
        return self.faker.random_int(min=self.min, max=self.max)


class ChoiceSpec(GenerationSpec):
    """
    Class for generating a value based on a list of choices.

    Inherits from:
        GenerationSpec: Abstract base class for generation specifications.

    Attributes:
        values (List[Any]): The list of values to choose from.
        n (int): The number of elements to pick. Defaults to 1.
        weights (Optional[List[float]]): The probabilities associated with each entry in values. Defaults to None.
    """

    def __init__(self, values: List[Any], n: int = 1, weights: Optional[List[float]] = None):
        """
        Initialize ChoiceSpec.

        Args:
            values (List[Any]): The list of values to choose from.
            n (int, optional): The number of elements to pick. Defaults to 1.
            weights (Optional[List[float]], optional): The probabilities associated with each entry in values. Defaults to None.
        """
        self.values = values
        self.weights = weights
        self.n = n
        self._validate_weights()
        super().__init__()

    def _validate_weights(self):
        """
        Validate the weights.

        Raises:
            ValueError: If the sum of the weights is not 1.
        """
        if self.weights:
            if sum(self.weights) != 1:
                raise ValueError(f'Weights must sum to 1, not {sum(self.weights)}')

    def value(self, *args, **kwargs):
        """
        Generate a value based on the choices and their weights.

        Returns:
            Any: The generated value. If n is 1, return the single picked value, otherwise return a list of picked values.
        """
        picked = choices(self.values, weights=self.weights, k=self.n)
        return picked[0] if self.n < 2 else picked


class FuncSpec(GenerationSpec):
    """
    Class for generating a value based on a function call.

    Inherits from:
        GenerationSpec: Abstract base class for generation specifications.

    Attributes:
        func (Callable): The function to call to generate the value.
        args (tuple): The positional arguments to pass to the function.
        kwargs (dict): The keyword arguments to pass to the function.
    """

    def __init__(self, func: Callable, *args, **kwargs):
        """
        Initialize FuncSpec.

        Args:
            func (Callable): The function to call to generate the value.
            *args: Variable length argument list to pass to the function.
            **kwargs: Arbitrary keyword arguments to pass to the function.
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs
        super().__init__()

    def value(self, *args, **kwargs):
        """
        Generate a value by calling the function with the specified arguments.

        Returns:
            The result of the function call.
        """
        return self.func(*self.args, **self.kwargs)


class ValueSpec(GenerationSpec):
    """
    Class for generating a specific value.

    Inherits from:
        GenerationSpec: Abstract base class for generation specifications.

    Attributes:
        _value (Any): The value to generate.
    """

    def __init__(self, value: Any):
        """
        Initialize ValueSpec.

        Args:
            value (Any): The value to generate.
        """
        self._value = value
        super().__init__()

    def value(self, *args, **kwargs):
        """
        Generate the specified value.

        Returns:
            The specified value.
        """
        return self._value


class MappingSpec(GenerationSpec):
    """
    Class for generating a value based on a mapping.

    Inherits from:
        GenerationSpec: Abstract base class for generation specifications.

    Attributes:
        mapping (Dict[Any, Any]): The mapping of values.
        mapping_source (str): The source of the mapping.
        default (Optional[Any]): The default value if the mapping does not exist.
    """

    def __init__(self, mapping: Dict[Any, Any], mapping_source: str, default: Optional[Any] = None):
        """
        Initialize MappingSpec.

        Args:
            mapping (Dict[Any, Any]): The mapping of values.
            mapping_source (str): The source of the mapping.
            default (Optional[Any], optional): The default value if the mapping does not exist. Defaults to None.
        """
        self.mapping = mapping
        self.mapping_source = mapping_source
        self.default = default
        super().__init__()

    def value(self, *args, **kwargs):
        pass


class DateTimeSpec(GenerationSpec):
    def __init__(self, start_date: Union[datetime, str], end_date: Union[datetime, str]):
        self.start_date = start_date
        self.end_date = end_date
        super().__init__()

    def value(self, *args, **kwargs):
        return self.faker.date_time_between(start_date=self.start_date, end_date=self.end_date)


class DateSpec(GenerationSpec):
    def __init__(self, start_date: Union[date, str], end_date: Union[date, str]):
        self.start_date = start_date
        self.end_date = end_date
        super().__init__()

    def value(self, *args, **kwargs):
        return self.faker.date_between(start_date=self.start_date, end_date=self.end_date)


def default_generator(t: T.DataType) -> GenerationSpec:
    """
    Generates a default value based on the provided PySpark DataType.

    This function maps PySpark DataTypes to instances of GenerationSpec subclasses, which are configured to generate
    data appropriate for their respective PySpark DataTypes. If the input DataType is not found in the mapping, it
    defaults to returning an instance of ValueSpec with value=None.

    Args:
        t (T.DataType): The PySpark DataType for which to generate a default value.

    Returns:
        GenerationSpec: An instance of a GenerationSpec subclass that can generate a default value for the provided DataType.
    """
    _end = datetime.now()
    _start = _end - timedelta(days=90)
    type_to_generator = {
        T.IntegerType(): RangeSpec(min_value=0, max_value=100),
        T.DoubleType(): RangeSpec(min_value=0, max_value=100, precision=2),
        T.StringType(): ChoiceSpec(values=['a', 'b', 'c']),
        T.BooleanType(): ChoiceSpec(values=[True, False]),
        T.DateType(): DateSpec(start_date=_start, end_date=_end),
        T.TimestampType(): DateTimeSpec(start_date=_start - timedelta(days=90), end_date=_end),
    }
    return type_to_generator.get(t, ValueSpec(value=None))  # type: ignore
