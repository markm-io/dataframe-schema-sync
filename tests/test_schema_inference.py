import pandas as pd
from sqlalchemy import JSON, Boolean, DateTime, Float, Integer, Text

# Adjust the import path as needed.
from dataframe_schema_sync import schema_inference


def test_infer_sqlalchemy_type_for_integers():
    s = pd.Series(["1", "2", "3"])
    sql_type, converted = schema_inference.SchemaInference.infer_sqlalchemy_type(s)
    assert isinstance(sql_type, Integer)
    # Converted series should be of nullable integer type.
    assert converted.dtype.name == "Int64"


def test_infer_sqlalchemy_type_for_floats():
    s = pd.Series(["1.0", "2.5", "3.2"])
    sql_type, converted = schema_inference.SchemaInference.infer_sqlalchemy_type(s)
    assert isinstance(sql_type, Float)
    # The converted series should be of float dtype.
    assert converted.dtype == float


def test_infer_sqlalchemy_type_for_boolean():
    s = pd.Series(["true", "false", "true"])
    sql_type, converted = schema_inference.SchemaInference.infer_sqlalchemy_type(s)
    assert isinstance(sql_type, Boolean)
    # Check that the conversion maps to booleans.
    assert converted.tolist() == [True, False, True]


def test_infer_sqlalchemy_type_for_text():
    s = pd.Series(["abc", "def", "ghi"])
    sql_type, converted = schema_inference.SchemaInference.infer_sqlalchemy_type(s)
    assert isinstance(sql_type, Text)
    # Converted series should be of object type.
    assert converted.dtype == object


def test_infer_sqlalchemy_type_for_json():
    s = pd.Series([{"a": 1}, {"b": 2}])
    sql_type, converted = schema_inference.SchemaInference.infer_sqlalchemy_type(s)
    assert isinstance(sql_type, JSON)
    # The series should be unchanged.
    assert converted.tolist() == [{"a": 1}, {"b": 2}]


def test_detect_and_convert_datetime_iso():
    # ISO 8601 format strings.
    s = pd.Series(["2023-01-01T00:00:00Z", "2023-06-01T12:34:56Z", None])
    converted, success = schema_inference.SchemaInference.detect_and_convert_datetime(s)
    assert success
    # For non-null values, ensure they are parsed as timestamps with tz info.
    for val in converted[converted.notna()]:
        assert pd.notnull(val)
        # Check that the timestamp is timezone-aware.
        assert pd.Timestamp(val).tzinfo is not None


def test_detect_and_convert_datetime_rfc():
    # RFC 2822 format strings.
    s = pd.Series(["Fri, 01 Jan 2021 00:00:00 +0000", "Wed, 01 Jun 2022 12:34:56 +0000", None])
    converted, success = schema_inference.SchemaInference.detect_and_convert_datetime(s)
    assert success
    for val in converted[converted.notna()]:
        assert pd.notnull(val)
        assert pd.Timestamp(val).tzinfo is not None


def test_detect_and_convert_datetime_invalid():
    # Non-date strings should not be converted.
    s = pd.Series(["not a date", "still not a date", None])
    converted, success = schema_inference.SchemaInference.detect_and_convert_datetime(s)
    assert not success
    pd.testing.assert_series_equal(converted.astype(str), s.astype(str))


def test_convert_dataframe_mixed_types():
    # Create a DataFrame with mixed types.
    df = pd.DataFrame(
        {
            "col1": ["123", "abc", 456],  # Mixed numeric and text -> should be treated as Text
            "col2": ["true", "false", "true"],  # Boolean strings -> Boolean type
            "col3": ["2021-01-01T00:00:00Z", "2021-02-02T12:00:00Z", None],  # Valid ISO dates -> DateTime
        }
    )
    converted_df, dtype_map = schema_inference.SchemaInference.convert_dataframe(df)
    # col1 should be inferred as Text
    assert isinstance(dtype_map["col1"], Text)
    # col2 should be inferred as Boolean
    assert isinstance(dtype_map["col2"], Boolean)
    # col3 should be inferred as DateTime
    assert isinstance(dtype_map["col3"], DateTime)

    # Validate DataFrame column conversions:
    assert converted_df["col1"].dtype == object
    assert converted_df["col2"].dtype == bool
    # Use isinstance to check for timezone-aware datetime dtype
    assert isinstance(converted_df["col3"].dtype, pd.DatetimeTZDtype)


def test_convert_dataframe_empty():
    df = pd.DataFrame()
    converted_df, dtype_map = schema_inference.SchemaInference.convert_dataframe(df)
    assert converted_df.empty
    assert dtype_map == {}


def test_sync_schema_overwrite(tmp_path):
    # Create a temporary schema file path.
    schema_file = tmp_path / "schema.yaml"
    # Create a DataFrame.
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    # Convert dataframe using the "overwrite" sync method.
    converted_df, dtype_map = schema_inference.SchemaInference.convert_dataframe(
        df, schema_file=str(schema_file), sync_method="overwrite"
    )
    # Load schema from file.
    loaded_schema = schema_inference.SchemaInference.load_schema_from_yaml(schema_file)
    # The schema from the file should match the inferred schema.
    for col in dtype_map:
        assert str(dtype_map[col]) == str(loaded_schema[col])


def test_sync_schema_update(tmp_path):
    # Create a temporary schema file path.
    schema_file = tmp_path / "schema.yaml"
    # First, create a schema file with one column.
    df_initial = pd.DataFrame(
        {
            "a": [1, 2, 3],
        }
    )
    _, dtype_map_initial = schema_inference.SchemaInference.convert_dataframe(
        df_initial, schema_file=str(schema_file), sync_method="overwrite"
    )
    # Now, create a new DataFrame that adds a new column.
    df_new = pd.DataFrame({"a": [4, 5, 6], "b": ["x", "y", "z"]})
    _, dtype_map_updated = schema_inference.SchemaInference.convert_dataframe(
        df_new, schema_file=str(schema_file), sync_method="update"
    )
    # The updated schema should include both columns.
    assert "a" in dtype_map_updated
    assert "b" in dtype_map_updated
