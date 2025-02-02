import os

from sqlalchemy import Integer, String

from dataframe_schema_sync.schema_io import SchemaIO


def test_save_and_load_schema():
    dtype_map = {"name": String(), "age": Integer()}
    filename = "test_schema.yaml"

    SchemaIO.save_schema_to_yaml(dtype_map, filename)
    assert os.path.exists(filename)

    loaded_schema = SchemaIO.load_schema_from_yaml(filename)
    assert isinstance(loaded_schema["name"], String)
    assert isinstance(loaded_schema["age"], Integer)

    os.remove(filename)
