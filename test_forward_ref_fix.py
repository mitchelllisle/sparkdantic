#!/usr/bin/env python3
"""Test script to verify that the forward reference fix works."""

import sys
import os

# Add sparkdantic to the path
sys.path.insert(0, '/Users/rjurney/Software/sparkdantic/src')

# Add weave project to the path for importing the models
sys.path.insert(0, '/Users/rjurney/Software/weave')

from abzu.baml_client.types import Company, Ticker, Exchange
from sparkdantic import SparkModel
from pyspark.sql.types import StructType

print("Testing Sparkdantic forward reference fix...")
print("=" * 60)

# Test 1: Basic model without forward refs
print("\nTest 1: Basic SparkModel (no forward refs)")
class SimpleModel(SparkModel):
    id: int
    name: str

try:
    schema = SimpleModel.model_spark_schema()
    print("✓ Simple model schema generated successfully")
    print(f"  Schema: {schema.simpleString()[:100]}...")
except Exception as e:
    print(f"✗ Failed: {e}")

# Test 2: Company model with forward references
print("\nTest 2: Company model with forward references")

# Rebuild models to resolve forward refs
Company.model_rebuild()
Ticker.model_rebuild()

class SparkTicker(Ticker, SparkModel):
    """Spark-compatible Ticker model."""
    pass

class SparkCompany(Company, SparkModel):
    """Spark-compatible Company model."""
    pass

try:
    # Try to create the schema
    company_schema = SparkCompany.model_spark_schema()
    print("✓ Company schema generated successfully!")
    print(f"  Schema fields: {', '.join([f.name for f in company_schema.fields][:5])}...")
    
    # Check if ticker field is properly handled
    ticker_field = next((f for f in company_schema.fields if f.name == 'ticker'), None)
    if ticker_field:
        print(f"  Ticker field type: {ticker_field.dataType.typeName()}")
        if hasattr(ticker_field.dataType, 'fields'):
            ticker_fields = [f.name for f in ticker_field.dataType.fields]
            print(f"  Ticker sub-fields: {', '.join(ticker_fields)}")
            
            # Check if exchange field exists in ticker
            exchange_field = next((f for f in ticker_field.dataType.fields if f.name == 'exchange'), None)
            if exchange_field:
                print(f"  Exchange field type: {exchange_field.dataType.typeName()}")
    
except Exception as e:
    print(f"✗ Failed to generate Company schema: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Direct instantiation test
print("\nTest 3: Direct instantiation with data")
try:
    test_company = SparkCompany(
        id=1,
        name="Test Company",
        description="Test Description"
    )
    schema = test_company.model_spark_schema()
    print("✓ Direct instantiation and schema generation successful")
except Exception as e:
    print(f"✗ Failed: {e}")

print("\n" + "=" * 60)
print("Testing complete!")