import pandas as pd
from sqlalchemy import JSON, Boolean, DateTime, Float, Integer, Text

from dataframe_schema_sync import schema_inference


def test_infer_sqlalchemy_type_for_integers():
    s = pd.Series(["1", "2", "3"])
    sql_type, converted = schema_inference.SchemaInference.infer_sqlalchemy_type(s)
    assert isinstance(sql_type, Integer)
    assert converted.dtype.name == "Int64"


def test_infer_sqlalchemy_type_for_floats():
    s = pd.Series(["1.0", "2.5", "3.2"])
    sql_type, converted = schema_inference.SchemaInference.infer_sqlalchemy_type(s)
    assert isinstance(sql_type, Float)
    assert converted.dtype == float


def test_infer_sqlalchemy_type_for_boolean():
    s = pd.Series(["true", "false", "true"])
    sql_type, converted = schema_inference.SchemaInference.infer_sqlalchemy_type(s)
    assert isinstance(sql_type, Boolean)
    assert converted.tolist() == [True, False, True]


def test_infer_sqlalchemy_type_for_text():
    s = pd.Series(["abc", "def", "ghi"])
    sql_type, converted = schema_inference.SchemaInference.infer_sqlalchemy_type(s)
    assert isinstance(sql_type, Text)
    assert converted.dtype == object


def test_infer_sqlalchemy_type_for_json():
    s = pd.Series([{"a": 1}, {"b": 2}])
    sql_type, converted = schema_inference.SchemaInference.infer_sqlalchemy_type(s)
    assert isinstance(sql_type, JSON)
    assert converted.tolist() == [{"a": 1}, {"b": 2}]


def test_detect_and_convert_datetime_iso():
    s = pd.Series(["2023-01-01T00:00:00Z", "2023-06-01T12:34:56Z", None])
    converted, success = schema_inference.SchemaInference.detect_and_convert_datetime(s)
    assert success
    for val in converted[converted.notna()]:
        assert pd.notnull(val)
        assert pd.Timestamp(val).tzinfo is not None


def test_detect_and_convert_datetime_rfc():
    s = pd.Series(["Fri, 01 Jan 2021 00:00:00 +0000", "Wed, 01 Jun 2022 12:34:56 +0000", None])
    converted, success = schema_inference.SchemaInference.detect_and_convert_datetime(s)
    assert success
    for val in converted[converted.notna()]:
        assert pd.notnull(val)
        assert pd.Timestamp(val).tzinfo is not None


def test_detect_and_convert_datetime_invalid():
    s = pd.Series(["not a date", "still not a date", None])
    converted, success = schema_inference.SchemaInference.detect_and_convert_datetime(s)
    assert not success
    pd.testing.assert_series_equal(converted.astype(str), s.astype(str))


def test_convert_dataframe_mixed_types():
    df = pd.DataFrame(
        {
            "col1": ["123", "abc", 456],
            "col2": ["true", "false", "true"],
            "col3": ["2021-01-01T00:00:00Z", "2021-02-02T12:00:00Z", None],
        }
    )
    converted_df, dtype_map = schema_inference.SchemaInference.convert_dataframe(df)
    assert isinstance(dtype_map["col1"], Text)
    assert isinstance(dtype_map["col2"], Boolean)
    assert isinstance(dtype_map["col3"], DateTime)
    assert converted_df["col1"].dtype == object
    assert converted_df["col2"].dtype == bool
    assert isinstance(converted_df["col3"].dtype, pd.DatetimeTZDtype)


def test_convert_dataframe_empty():
    df = pd.DataFrame()
    converted_df, dtype_map = schema_inference.SchemaInference.convert_dataframe(df)
    assert converted_df.empty
    assert dtype_map == {}
