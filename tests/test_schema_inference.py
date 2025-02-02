import pandas as pd
import pytest
from sqlalchemy import Boolean, DateTime, Float, Integer, Text
from sqlalchemy.dialects.postgresql import JSON

from dataframe_schema_sync.schema_inference import SchemaInference


@pytest.mark.parametrize(
    "data, expected_type",
    [
        (pd.Series(["2024-01-01T12:30:45Z", "2025-02-02T15:00:00Z"]), DateTime(timezone=True)),
        (pd.Series(["Fri, 03 Jan 2025 00:10:04 GMT", "Sat, 04 Jan 2025 12:34:56 GMT"]), DateTime(timezone=True)),
        (pd.Series([1, 2, 3]), Integer()),
        (pd.Series([1.1, 2.2, 3.3]), Float()),
        (pd.Series(["true", "false", "yes", "no"]), Boolean()),
        (pd.Series(["hello", "world"]), Text()),
        (pd.Series([{"a": 1}, {"b": 2}]), JSON()),
        (pd.Series([None, None, None]), Text()),
    ],
)
def test_infer_sqlalchemy_type(data, expected_type):
    inferred_type, transformed_series = SchemaInference.infer_sqlalchemy_type(data)
    assert isinstance(inferred_type, type(expected_type))


def test_detect_and_convert_datetime_iso():
    series = pd.Series(["2024-01-01T12:30:45Z", "2025-02-02T15:00:00Z", None])
    converted, is_datetime = SchemaInference.detect_and_convert_datetime(series)
    assert is_datetime
    assert pd.api.types.is_datetime64_any_dtype(converted)


def test_detect_and_convert_datetime_rfc():
    series = pd.Series(["Fri, 03 Jan 2025 00:10:04 GMT", "Sat, 04 Jan 2025 12:34:56 GMT"])
    converted, is_datetime = SchemaInference.detect_and_convert_datetime(series)
    assert is_datetime
    assert pd.api.types.is_datetime64_any_dtype(converted)


def test_detect_and_convert_datetime_invalid():
    series = pd.Series(["not a date", "still not a date"])
    converted, is_datetime = SchemaInference.detect_and_convert_datetime(series)
    assert not is_datetime
    assert converted.equals(series)


def test_infer_sqlalchemy_type_for_booleans():
    series = pd.Series(["true", "false", "yes", "no", "1", "0"])
    inferred_type, transformed_series = SchemaInference.infer_sqlalchemy_type(series)
    assert isinstance(inferred_type, Boolean)
    assert set(transformed_series.dropna().unique()) == {True, False}


def test_infer_sqlalchemy_type_for_mixed_boolean_strings():
    series = pd.Series(["True", "no", "random", "0", "1"])
    inferred_type, transformed_series = SchemaInference.infer_sqlalchemy_type(series)
    assert isinstance(inferred_type, Text)  # Since "random" is not a boolean value


def test_infer_sqlalchemy_type_for_integers():
    series = pd.Series([1, 2, 3, None])
    inferred_type, transformed_series = SchemaInference.infer_sqlalchemy_type(series)
    assert isinstance(inferred_type, Integer)
    assert pd.api.types.is_integer_dtype(transformed_series)


def test_infer_sqlalchemy_type_for_floats():
    series = pd.Series([1.1, 2.2, 3.3])
    inferred_type, transformed_series = SchemaInference.infer_sqlalchemy_type(series)
    assert isinstance(inferred_type, Float)
    assert pd.api.types.is_float_dtype(transformed_series)


def test_infer_sqlalchemy_type_for_mixed_numbers():
    series = pd.Series(["1", "2.5", "text", None])
    inferred_type, transformed_series = SchemaInference.infer_sqlalchemy_type(series)
    assert isinstance(inferred_type, Text)  # Since "text" forces it to be non-numeric


def test_infer_sqlalchemy_type_for_json():
    series = pd.Series([{"a": 1}, {"b": 2}])
    inferred_type, transformed_series = SchemaInference.infer_sqlalchemy_type(series)
    assert isinstance(inferred_type, JSON)


def test_infer_sqlalchemy_type_for_text():
    series = pd.Series(["hello", "world"])
    inferred_type, transformed_series = SchemaInference.infer_sqlalchemy_type(series)
    assert isinstance(inferred_type, Text)
    assert pd.api.types.is_string_dtype(transformed_series)


def test_convert_dataframe_mixed_types():
    df = pd.DataFrame({"mixed_column": ["123", "abc", 456]})

    converted_df, dtype_map = SchemaInference.convert_dataframe(df)

    print(f"Inferred type for 'mixed_column': {dtype_map['mixed_column']}")  # Debugging output

    # Check if the inferred type is correctly identified as Text
    assert isinstance(dtype_map["mixed_column"], Text), f"Expected Text, but got {dtype_map['mixed_column']}"


def test_convert_dataframe():
    df = pd.DataFrame(
        {
            "dates_iso": ["2024-01-01T12:30:45Z", "2025-02-02T15:00:00Z"],
            "integers": [1, 2],
            "floats": [1.1, 2.2],
            "text": ["hello", "world"],
            "booleans": ["true", "false"],
            "json": [{"a": 1}, {"b": 2}],
        }
    )

    converted_df, dtype_map = SchemaInference.convert_dataframe(df)

    assert isinstance(dtype_map["dates_iso"], DateTime)
    assert dtype_map["dates_iso"].timezone is True
    assert isinstance(dtype_map["integers"], Integer)
    assert isinstance(dtype_map["floats"], Float)
    assert isinstance(dtype_map["text"], Text)
    assert isinstance(dtype_map["booleans"], Boolean)
    assert isinstance(dtype_map["json"], JSON)

    assert pd.api.types.is_datetime64_any_dtype(converted_df["dates_iso"])
    assert pd.api.types.is_integer_dtype(converted_df["integers"])
    assert pd.api.types.is_float_dtype(converted_df["floats"])
    assert pd.api.types.is_string_dtype(converted_df["text"])
    assert set(converted_df["booleans"].dropna().unique()) == {True, False}


def test_convert_dataframe_empty():
    df = pd.DataFrame()
    converted_df, dtype_map = SchemaInference.convert_dataframe(df)
    assert converted_df.empty
    assert dtype_map == {}
