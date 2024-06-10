##  SparkDantic

[![codecov](https://codecov.io/gh/mitchelllisle/sparkdantic/graph/badge.svg?token=O6PPQX4FEX)](https://codecov.io/gh/mitchelllisle/sparkdantic)
[![PyPI version](https://badge.fury.io/py/sparkdantic.svg)](https://badge.fury.io/py/sparkdantic)

> 1️⃣ version: 0.26.0

> ✍️ author: Mitchell Lisle

# PySpark Model Conversion Tool

This Python module provides a utility for converting Pydantic models to PySpark schemas. It's implemented as a class 
named `SparkModel` that extends the Pydantic's `BaseModel`.

## Features

- Conversion from Pydantic model to PySpark schema.
- Generate fake data from your model with optional configuration of each field

## Usage

### Creating a new SparkModel

A `SparkModel` is a Pydantic model, and you can define one by simply inheriting from `SparkModel` and defining some fields:

```python
from sparkdantic import SparkModel
from typing import List

class MyModel(SparkModel):
    name: str
    age: int
    hobbies: List[str]
```

> ℹ️ `Enum`s are supported but they must be mixed with either `int` (`IntEnum` in Python ≥ 3.10) or `str` (`StrEnum`, in Python ≥ 3.11) built-in types:

```python
from enum import Enum

class Switch(int, Enum):
    OFF = 0
    ON = 1

class MyEnumModel(SparkModel):
    switch: Switch
```

### Generating a PySpark Schema

Pydantic has existing models for generating json schemas (with `model_json_schema`). With a `SparkModel` you can 
generate a PySpark schema from the model fields using the `model_spark_schema()` method:

```python
spark_schema = MyModel.model_spark_schema()
```

Provides this schema:

```python
StructType([
    StructField('name', StringType(), False),
    StructField('age', IntegerType(), False),
    StructField('hobbies', ArrayType(StringType(), False), False)
])
```
> ℹ️  In addition to the automatic type conversion, you can also explicitly coerce data types to Spark native types by 
>  setting the `spark_type` attribute in the `Field` function from Pydantic, like so: `Field(spark_type=DataType)`.
>  Please replace DataType with the actual Spark data type you want to use.
>  This is useful when you want to use a specific data type then the one that Sparkdantic infers by default. 


### Generating fake data
Once you've defined a schema you can generate some fake data for your class. Useful for testing purposes as well as for
populating development environments with non-production data. 

Using the same schema above:
```python
from pyspark.sql import SparkSession
from sparkdantic import SparkModel
from typing import List

class MyModel(SparkModel):
    name: str
    age: int
    hobbies: List[str]
    
spark = SparkSession.builder.getOrCreate()
    
fake_data = MyModel.generate_data(spark)
```
Using the defaults you'll get fake data that looks something like this (as a Spark DataFrame):

```python
Row(name='0', age=0, hobbies=['0']), 
Row(name='1', age=1, hobbies=['1']), 
Row(name='2', age=2, hobbies=['2']), 
Row(name='3', age=3, hobbies=['3']), 
Row(name='4', age=4, hobbies=['4']), 
Row(name='5', age=5, hobbies=['5']), 
Row(name='6', age=6, hobbies=['6']), 
Row(name='7', age=7, hobbies=['7']), 
Row(name='8', age=8, hobbies=['8']), 
Row(name='9', age=9, hobbies=['9']),
```

Definitely fake data, but not very useful for replicating the real world. We can provide some more info so that it generates
some more realistic figures:

```python
from pyspark.sql import SparkSession
from sparkdantic import SparkModel, ColumnGenerationSpec
from typing import List

class MyModel(SparkModel):
    name: str
    age: int
    hobbies: List[str]
    
spark = SparkSession.builder.getOrCreate()

specs = {
    'name': ColumnGenerationSpec(values=['Bob', 'Alice'], weights=[0.5, 0.5]),
    'age': ColumnGenerationSpec(min_value=20, max_value=65),
    'hobbies': ColumnGenerationSpec(values=['music', 'movies', 'sport'], num_features=2)
}

fake_data = MyModel.generate_data(spark, specs=specs)
```

Let's breakdown what we've generated:
- `name`: We give it a list of two possible values for this field. The weights value determines how often to choose a value.
    In our case, it chooses one of the two values an equal amount (50% of the rows will be Bob, 50% Alice)
- `age`: Choose an int between 20 and 65
- `hobbies`: Choose a value from the list at random

There are plenty more options available. Have a look at the [library we wrap for this functionality](https://databrickslabs.github.io/dbldatagen/public_docs/index.html) 
for more examples and information on what you can generate

```python
Row(name='Bob', age=20, hobbies=['music']),
Row(name='Bob', age=21, hobbies=['movies']),
Row(name='Bob', age=22, hobbies=['sport']),
Row(name='Bob', age=23, hobbies=['music']),
Row(name='Bob', age=24, hobbies=['movies']),
Row(name='Alice', age=25, hobbies=['sport']),
Row(name='Alice', age=26, hobbies=['music']),
Row(name='Alice', age=27, hobbies=['movies']),
Row(name='Alice', age=28, hobbies=['sport']),
Row(name='Alice', age=29, hobbies=['music'])
```


## Contributing
Contributions welcome! If you would like to add a new feature / fix a bug feel free to raise a PR and tag me (`mitchelllisle`) as
a reviewer. Please setup your environment locally to ensure all styling and development flow is as close to the standards set in
this project as possible. To do this, the main thing you'll need is `poetry`. You should also run `make install-dev-local` which 
will install the `pre-commit-hooks` as well as install the project locally. PRs won't be accepted without sufficient tests and 
we will be strict on maintaining a 100% test coverage.

> ℹ️ Note that after you have run `make install-dev-local` and make a commit we run the test suite as part of the pre-commit 
> hook checks. This is to ensure you don't commit code that breaks the tests. This will also try and commit changes to 
> the COVERAGE.txt file so that we can compare coverage in each PR. Please ensure this file is commited with your changes

> ℹ️ Versioning: We use `bumpversion` to maintain the version across various files. If you submit a PR please run bumpversion to
> the following rules:
> - `bumpversion major`: If you are making breaking changes (that is, anyone who already uses this library can no longer rely on
> existing methods / functionality)
> - `bumpversion minor`: If you are adding functionality or features that maintain existing methods and features
> - `bumpversion patch`: If you are fixing a bug or making some other small change

> Note: ⚠️ You can ignore bumping the version if you like. I periodically do releases of any dependency updates anyway so
> if you can wait a couple of days for your code to be pushed to PyPi then just submit the change and I'll make sure it's
> included in the next release. I'll do my best to make sure it's released ASAP after your PR is merged.