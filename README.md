##  SparkDantic

[![codecov](https://codecov.io/gh/mitchelllisle/sparkdantic/graph/badge.svg?token=O6PPQX4FEX)](https://codecov.io/gh/mitchelllisle/sparkdantic)
[![PyPI version](https://badge.fury.io/py/sparkdantic.svg)](https://badge.fury.io/py/sparkdantic)

> 1️⃣ version: 2.5.0

> ✍️ author: Mitchell Lisle

# PySpark Model Conversion Tool

This Python module provides a utility for converting Pydantic models to PySpark schemas. It's implemented as a class 
named `SparkModel` that extends the Pydantic's `BaseModel`.

## Features

- Conversion from Pydantic model to PySpark schema (`StructType` or JSON)
- Type coercion
- PySpark as an optional dependency

## Installation

Without PySpark:

```shell
pip install sparkdantic
```

> Note: only **JSON** schema generation features are available without PySpark installed. If you attempt to use PySpark-dependent schema generation features, SparkDantic will check that a supported version of Pyspark is installed.

With PySpark:

```shell
pip install "sparkdantic[pyspark]"
```

### Supported PySpark versions

`3.3.0` or higher, but not `4.0`

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

> ℹ️ A field can be excluded from the Spark schema using the pydantic's `exclude` Field attribute. This is
> useful when e.g. the pydantic model has Spark incompatible fields. Note that `exclude` is a pydantic field 
> attribute and not a sparkdantic feature. Setting it will exclude the field from any pydantic serialisation/deserialisation.

```python
from pydantic import Field
from sparkdantic import SparkModel
from typing import Any

class MyModel(SparkModel):
    name: str
    age: int
    arbitrary_data: Any = Field(exclude=True)

```
Running `MyModel.model_spark_schema(exclude_fields=True)` should return the following schema:

```python
StructType([
    StructField('name', StringType(), False),
    StructField('age', IntegerType(), False)
])
```

Calling `model_spark_schema` without the option raises exception due to incompatible types.
### Generating a PySpark Schema

#### Using `SparkModel`

Pydantic has existing models for generating [JSON schemas](https://docs.pydantic.dev/2.10/concepts/json_schema/) (with `model_json_schema`). With a `SparkModel` you can 
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

You can also generate a [PySpark-compatible JSON schema](https://spark.apache.org/docs/3.5.4/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html#pyspark.sql.types.StructType.fromJson) from the model fields using the `model_json_spark_schema` method:

```python
json_spark_schema = MyModel.model_json_spark_schema()
```

Provides this schema:

```python
{
    "type": "struct",
    "fields": [
        {
            "name": "name",
            "type": "string",
            "nullable": False,
            "metadata": {}
        },
        {
            "name": "age",
            "type": "integer",
            "nullable": False,
            "metadata": {}
        },
        {
            "name": "hobbies",
            "type": {
                "type": "array",
                "elementType": "string",
                "containsNull": False
            },
            "nullable": False,
            "metadata": {}
        }
    ]
}
```

#### Using Pydantic `BaseModel`

You can also generate a PySpark schema for existing Pydantic models using the `create_spark_schema` function:

```python
from sparkdantic import create_spark_schema, create_json_spark_schema

class EmployeeModel(BaseModel):
    id: int
    first_name: str
    last_name: str
    department_code: str

spark_schema = create_spark_schema(EmployeeModel)
json_spark_schema = create_json_spark_schema(EmployeeModel)
```

> ℹ️  In addition to the automatic type conversion, you can also explicitly coerce data types to Spark native types by 
>  setting the `spark_type` attribute in the `SparkField` function (which extends the Pydantic `Field` function), like so: `SparkField(spark_type=<datatype>)`.
>  `datatype` accepts `str` (e.g. `"bigint"`) or `pyspark.sql.types.DataType` (e.g. `LongType`).
>  This is useful when you want to use a specific data type then the one that Sparkdantic infers by default. 

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
