##  SparkDantic

[![codecov](https://codecov.io/gh/mitchelllisle/sparkdantic/graph/badge.svg?token=O6PPQX4FEX)](https://codecov.io/gh/mitchelllisle/sparkdantic)
[![PyPI version](https://badge.fury.io/py/sparkdantic.svg)](https://badge.fury.io/py/sparkdantic)

> 1️⃣ version: 0.8.0

> ✍️ author: Mitchell Lisle

# PySpark Model Conversion Tool

This Python module provides a utility for converting Pydantic models to PySpark schemas. It's implemented as a class 
named `SparkModel` that extends the Pydantic's `BaseModel`.

## Features

- Conversion from Pydantic model to PySpark schema.
- Determination of nullable types.
- Customizable type mapping between Python and PySpark data types.

## Dependencies

This module aims to have a small dependency footprint:
- `pydantic`
- `pyspark`
- Python's built-in `datetime`, `decimal`, `types`, and `typing` modules

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

### Generating a PySpark Schema

Pydantic has existing models for generating json schemas (with `model_json_schema`). With a `SparkModel` you can 
generate a PySpark schema from the model fields using the `model_spark_schema()` method:

```python
my_model = MyModel()
spark_schema = my_model.model_spark_schema()
```

Provides this schema:

```python
StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('hobbies', ArrayType(StringType(), False), True)
])
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
> `bumpversion major`: If you are making breaking changes (that is, anyone who already uses this library can no longer rely on
> existing methods / functionality)
> `bumpversion minor`: If you are adding functionality or features that maintain existing methods and features
> `bumpversion patch`: If you are fixing a bug or making some other small change