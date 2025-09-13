"""Test handling of forward references in Pydantic models."""

import pytest
from enum import Enum
from typing import Optional

from pydantic import BaseModel

from sparkdantic import SparkModel


class TestForwardReferences:
    """Test suite for forward reference handling."""

    def test_model_with_forward_reference(self):
        """Test that models with forward references can be converted to Spark schema."""
        
        # Define models with forward references
        class Status(str, Enum):
            ACTIVE = "ACTIVE"
            INACTIVE = "INACTIVE"
        
        class NestedModel(BaseModel):
            id: int
            status: Optional[Status] = None
        
        class ParentModel(BaseModel):
            id: int
            name: str
            # This is a forward reference
            nested: Optional["NestedModel"] = None
        
        # Rebuild to resolve forward references
        ParentModel.model_rebuild()
        NestedModel.model_rebuild()
        
        # Create SparkModel versions
        class SparkNestedModel(NestedModel, SparkModel):
            pass
        
        class SparkParentModel(ParentModel, SparkModel):
            pass
        
        # Should be able to generate schema without errors
        schema = SparkParentModel.model_spark_schema()
        
        # Verify the schema structure
        assert schema is not None
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "name" in field_names
        assert "nested" in field_names
        
        # Check that nested field is properly resolved
        nested_field = next(f for f in schema.fields if f.name == "nested")
        assert nested_field.dataType.typeName() == "struct"
        
        # Check nested struct has the expected fields
        nested_fields = [f.name for f in nested_field.dataType.fields]
        assert "id" in nested_fields
        assert "status" in nested_fields

    def test_model_with_enum_in_nested_model(self):
        """Test that enums in nested models are handled correctly."""
        
        class Color(str, Enum):
            RED = "RED"
            GREEN = "GREEN"
            BLUE = "BLUE"
        
        class Item(BaseModel):
            name: str
            color: Optional[Color] = None
        
        class Container(BaseModel):
            id: int
            item: Optional["Item"] = None
        
        # Rebuild models
        Container.model_rebuild()
        Item.model_rebuild()
        
        class SparkItem(Item, SparkModel):
            pass
        
        class SparkContainer(Container, SparkModel):
            pass
        
        # Generate schema
        schema = SparkContainer.model_spark_schema()
        
        # Verify structure
        assert schema is not None
        item_field = next(f for f in schema.fields if f.name == "item")
        assert item_field.dataType.typeName() == "struct"
        
        # Check that color field in nested item is string (enum converted)
        color_field = next(f for f in item_field.dataType.fields if f.name == "color")
        assert color_field.dataType.typeName() == "string"

    def test_model_rebuild_with_undefined_reference(self):
        """Test that model_rebuild with undefined references doesn't crash."""
        
        class ModelWithBadRef(BaseModel):
            id: int
            # This forward reference won't resolve
            bad_ref: Optional["NonExistentModel"] = None
        
        class SparkModelWithBadRef(ModelWithBadRef, SparkModel):
            pass
        
        # Should handle gracefully even if forward ref can't be resolved
        # The field should default to string type
        schema = SparkModelWithBadRef.model_spark_schema()
        
        assert schema is not None
        bad_ref_field = next(f for f in schema.fields if f.name == "bad_ref")
        # When forward ref can't be resolved, it defaults to string
        assert bad_ref_field.dataType.typeName() == "string"

    def test_circular_reference_handling(self):
        """Test that circular references are handled properly."""
        
        class Node(BaseModel):
            value: int
            # Circular reference to self
            next_node: Optional["Node"] = None
        
        # Rebuild to resolve the self-reference
        Node.model_rebuild()
        
        class SparkNode(Node, SparkModel):
            pass
        
        # This should work without infinite recursion
        schema = SparkNode.model_spark_schema()
        
        assert schema is not None
        next_node_field = next(f for f in schema.fields if f.name == "next_node")
        # Circular refs should be resolved to struct type
        assert next_node_field.dataType.typeName() == "struct"