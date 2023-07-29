##  SparkDantic

> 1️⃣ version: 0.1.0

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
