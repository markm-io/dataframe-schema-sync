import json
from collections.abc import Iterator
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, ClassVar, Optional, Union

import numpy as np
import pandas as pd
import yaml  # type: ignore
from sqlalchemy import Boolean, DateTime, Float, Integer, Text
from sqlalchemy.dialects.postgresql import JSON


class SchemaInference:
    """
    A class for inferring SQLAlchemy types from Pandas DataFrame columns and handling schema I/O operations.
    """

    SQLALCHEMY_TYPE_MAP: ClassVar[dict[str, Any]] = {
        "TIMESTAMP(timezone=True)": DateTime(timezone=True),
        "DATETIME": DateTime(timezone=True),
        "INTEGER": Integer(),
        "FLOAT": Float(),
        "TEXT": Text(),
        "JSON": JSON,
        "BOOLEAN": Boolean(),
    }

    @staticmethod
    def safe_str_conversion(x: Any) -> str:
        """
        Convert a value to string, returning an empty string if the value is NaN.

        Args:
            x (Any): The value to convert.

        Returns:
            str: The string representation or an empty string if x is NaN.
        """
        return "" if pd.isna(x) else str(x)

    @staticmethod
    def safe_json_conversion(x: Any) -> list[Any]:
        """
        Convert a value to a JSON object if it's a string. If the value is missing (NaN),
        empty, or cannot be converted, return an empty array.

        Args:
            x (Any): The value to convert.

        Returns:
            list: The parsed JSON value or an empty list.
        """
        # If the value is a string, attempt to parse it as JSON.
        if isinstance(x, str):
            try:
                return json.loads(x)
            except Exception:
                return []
        # If the value is None, return an empty list.
        if x is None:
            return []
        # For numeric types or other types, try to detect if it's missing.
        if pd.isna(x):
            return []
        # If x is a numpy array, check its size.
        if isinstance(x, np.ndarray):
            if x.size == 0:
                return []
            else:
                return x
        # If x is a list, check if it's empty.
        if isinstance(x, list):
            if len(x) == 0:
                return []
            else:
                return x
        return x

    @staticmethod
    def save_schema_to_yaml(
        dtype_map: dict[str, Any],
        filename: Union[str, Path],
        schema_name: str,
        mapping_key: str,
        sync_method: str,
    ) -> None:
        """
        Save dtype_map to a YAML file, storing SQLAlchemy types as text strings.
        The YAML content will be nested under the provided schema_name key and further under the given mapping_key.
        The sync_method parameter determines how the new schema is saved:
          - "update": Only new columns (not already present under mapping_key) are added.
          - "overwrite": The mapping for mapping_key is completely replaced by the new schema.

        Args:
            dtype_map (dict): Dictionary mapping column names to SQLAlchemy types.
            filename (str or Path): Path to the YAML file.
            schema_name (str): The parent key to use in the YAML file.
            mapping_key (str): The key under which the schema items are stored.
            sync_method (str): Must be either "update" or "overwrite".

        Raises:
            ValueError: If sync_method is not one of the allowed values.
        """
        if sync_method not in ("update", "overwrite"):
            raise ValueError("sync_method must be either 'update' or 'overwrite'")

        schema_path = Path(filename)
        # Serialize the dtype_map (convert SQLAlchemy types to strings)
        new_mapping = {
            col: "TEXT" if isinstance(sql_type, Text) else str(sql_type) for col, sql_type in dtype_map.items()
        }

        if schema_path.exists():
            with open(filename, encoding="utf-8") as file:
                content: dict[str, Any] = yaml.safe_load(file) or {}
        else:
            content = {}

        if schema_name in content:
            # There is an existing schema block
            existing_mapping = content[schema_name].get(mapping_key, {})
            if sync_method == "update":
                # Only add new columns that are not already defined
                merged_mapping = existing_mapping.copy()
                for col, val in new_mapping.items():
                    if col not in merged_mapping:
                        merged_mapping[col] = val
                content[schema_name][mapping_key] = merged_mapping
            else:  # overwrite
                content[schema_name][mapping_key] = new_mapping
        else:
            # No existing schema block; add one
            content[schema_name] = {mapping_key: new_mapping}

        with open(filename, "w", encoding="utf-8") as file:
            yaml.dump(content, file, sort_keys=False)

    @staticmethod
    def load_schema_from_yaml(
        filename: Union[str, Path],
        schema_name: str,
        mapping_key: str,
    ) -> dict[str, Any]:
        """
        Load schema from a YAML file and convert stored text strings back into SQLAlchemy types.
        The method looks for the schema under the provided schema_name key and within its dynamic mapping_key.

        Args:
            filename (str or Path): Path to the YAML file.
            schema_name (str): The parent key under which the schema is stored.
            mapping_key (str): The key under which the schema items are stored.

        Returns:
            dict: Dictionary mapping column names to SQLAlchemy types.

        Raises:
            KeyError: If the provided schema_name or the mapping_key is not found in the YAML file.
        """
        with open(filename, encoding="utf-8") as file:
            loaded_content: dict[str, Any] = yaml.safe_load(file) or {}

        loaded_schema = loaded_content.get(schema_name)
        if loaded_schema is None:
            raise KeyError(f"Schema '{schema_name}' not found in {filename}")

        columns_mapping = loaded_schema.get(mapping_key)
        if columns_mapping is None:
            raise KeyError(f"'{mapping_key}' key not found under schema '{schema_name}' in {filename}")

        return {
            col: SchemaInference.SQLALCHEMY_TYPE_MAP.get(sql_type or "TEXT", Text())
            for col, sql_type in columns_mapping.items()
        }

    @staticmethod
    def load_config_from_yaml(
        filename: Union[str, Path],
        schema_name: str,
        mapping_key: str,
    ) -> dict[str, Any]:
        """
        Load config from a YAML file and convert stored text strings back into SQLAlchemy types.
        The method looks for the schema under the provided schema_name key and within its dynamic mapping_key.

        Args:
            filename (str or Path): Path to the YAML file.
            schema_name (str): The parent key under which the schema is stored.
            mapping_key (str): The key under which the schema items are stored.

        Returns:
            dict: Dictionary mapping column names to SQLAlchemy types.

        Raises:
            KeyError: If the provided schema_name or the mapping_key is not found in the YAML file.
        """
        with open(filename, encoding="utf-8") as file:
            loaded_content: dict[str, Any] = yaml.safe_load(file) or {}

        loaded_schema = loaded_content.get(schema_name)
        if loaded_schema is None:
            raise KeyError(f"Schema '{schema_name}' not found in {filename}")

        columns_mapping = loaded_schema.get(mapping_key)
        if columns_mapping is None:
            raise KeyError(f"'{mapping_key}' key not found under schema '{schema_name}' in {filename}")

        return columns_mapping
        # return {
        #     col: SchemaInference.SQLALCHEMY_TYPE_MAP.get(sql_type or "TEXT", Text())
        #     for col, sql_type in columns_mapping.items()
        # }

    @staticmethod
    def detect_and_convert_datetime(series: pd.Series) -> tuple[pd.Series, bool]:
        """
        Detects datetime format in a Pandas Series and converts it to datetime64[ns, UTC].
        Supports:
          - ISO 8601 formats (YYYY-MM-DDTHH:MM:SS.sssZ)
          - RFC 2822 (email/HTTP format)
          - Standard datetime format (YYYY-MM-DD HH:MM:SS.sss)
        Returns the converted series and a boolean indicating success.
        """
        # Use only non-null values for detection.
        non_null = series.dropna()
        if non_null.empty:
            return pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns, UTC]"), False

        # Check if series looks like a standard datetime (YYYY-MM-DD HH:MM:SS.sss)
        # This format check comes first to catch your specific format
        if isinstance(non_null.iloc[0], str) and len(non_null.iloc[0]) >= 19:
            sample = non_null.iloc[0]
            if (
                len(sample) >= 19
                and sample[4] == "-"
                and sample[7] == "-"
                and sample[10] == " "
                and sample[13] == ":"
                and sample[16] == ":"
            ):
                parsed_dates = pd.to_datetime(non_null, format="%Y-%m-%d %H:%M:%S.%f", errors="coerce", utc=True)
                if parsed_dates.notna().all():
                    # Apply conversion to the full series
                    result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns, UTC]")
                    result[series.notna()] = pd.to_datetime(
                        series[series.notna()], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce", utc=True
                    )
                    return result, True

        # Check only the non-null values.
        if parsed_dates.notna().all():
            result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns, UTC]")
            result[series.notna()] = pd.to_datetime(series[series.notna()], errors="coerce", utc=True)
            return result, True

        # Attempt RFC 2822 conversion
        parsed_dates = series.apply(
            lambda x: parsedate_to_datetime(x).astimezone(pd.Timestamp.utc)
            if pd.notnull(x) and isinstance(x, str)
            else pd.NaT
        )
        if parsed_dates[series.notna()].notna().all():
            return parsed_dates, True

        parsed_dates = series.apply(
            lambda date_str: pd.NaT
            if date_str == "0001-01-01T00:00:00Z"
            else pd.to_datetime(date_str, errors="coerce", utc=True)
        )
        if parsed_dates[series.apply(lambda x: x != "0001-01-01T00:00:00Z")].notna().all():
            return parsed_dates, True

        # If not all non-null values can be converted, do not treat the series as datetime.
        return series, False

    @staticmethod
    def infer_sqlalchemy_type(
        series: pd.Series, infer_dates: bool = True, date_columns: Optional[Union[str, list[str]]] = None
    ) -> tuple[Any, pd.Series]:
        """
        Given a pandas Series, determine the best matching SQLAlchemy type.

        Args:
            series (pd.Series): The column to analyze.
            infer_dates (bool): If True, attempts to infer datetime columns.
            date_columns (str or list of str): Specifies columns that should always be parsed as dates.

        Returns:
            tuple: (SQLAlchemy type, transformed Pandas Series)
        """
        # Normalize date_columns to a list if provided
        date_cols_list = []
        if date_columns is not None:
            if isinstance(date_columns, str):
                date_cols_list = [date_columns]
            else:
                date_cols_list = date_columns

        non_null = series.dropna()
        if non_null.empty:
            # When there is no non-null value, default to Text and use safe conversion so NaN becomes an empty cell.
            return Text(), series.apply(SchemaInference.safe_str_conversion)

        # If column is already datetime type, return DateTime type
        if pd.api.types.is_datetime64_any_dtype(series):
            # Ensure timezone awareness
            if series.dt.tz is None:
                return DateTime(timezone=True), pd.to_datetime(series, utc=True)
            return DateTime(timezone=True), series

        sample = non_null.iloc[0]

        # --- JSON-like objects ---
        if isinstance(sample, (dict, list)):
            return JSON(), series

        # --- Boolean conversion ---
        if pd.api.types.is_bool_dtype(series):
            return Boolean(), series

        true_vals = {"true", "t"}
        false_vals = {"false", "f"}
        lower_non_null = non_null.astype(str).str.lower().str.strip()

        if lower_non_null.isin(true_vals.union(false_vals)).all():
            converted = series.astype(str).str.lower().str.strip().map(lambda x: x in true_vals)
            return Boolean(), converted

        # --- DateTime Conversion --- (Moved before numeric conversion)
        if infer_dates or (date_cols_list and series.name in date_cols_list):
            # Quick check if this looks like a datetime string before attempting full conversion
            if isinstance(sample, str) and len(sample) >= 19:
                # Check for common datetime pattern YYYY-MM-DD HH:MM:SS
                if (
                    sample[4] == "-"
                    and sample[7] == "-"
                    and (sample[10] == " " or sample[10] == "T")
                    and sample[13] == ":"
                    and sample[16] == ":"
                ):
                    converted_series, is_datetime = SchemaInference.detect_and_convert_datetime(series)
                    if is_datetime:
                        return DateTime(timezone=True), converted_series

            # If not caught by the quick check but specified as a date column, try conversion anyway
            if date_cols_list and series.name in date_cols_list:
                converted_series, is_datetime = SchemaInference.detect_and_convert_datetime(series)
                if is_datetime:
                    return DateTime(timezone=True), converted_series

        # --- Numeric conversion ---
        # Replace silent try-except-pass with proper handling
        numeric_conversion_successful = False
        try:
            numeric_series = pd.to_numeric(series, errors="raise")
            numeric_conversion_successful = True
        except (ValueError, TypeError):
            # Cannot convert to numeric type, continuing to other type checks
            numeric_conversion_successful = False

        if numeric_conversion_successful:
            if numeric_series.dropna().apply(lambda x: float(x).is_integer()).all():
                return Integer(), numeric_series.astype("Int64")
            return Float(), numeric_series.astype(float)

        # --- Final DateTime Attempt --- (For cases not caught by the quick check)
        if infer_dates and not (date_cols_list and series.name in date_cols_list):
            converted_series, is_datetime = SchemaInference.detect_and_convert_datetime(series)
            if is_datetime:
                return DateTime(timezone=True), converted_series

        # --- Fallback to text ---
        return Text(), series.apply(SchemaInference.safe_str_conversion)

    @staticmethod
    def convert_dataframe(
        df: pd.DataFrame,
        infer_dates: bool = True,
        date_columns: Optional[Union[str, list[str]]] = None,
        case: str = "snake",
        truncate_limit: int = 55,
    ) -> "SchemaConversionResult":
        """
        Infer the SQLAlchemy type for each column, convert the DataFrame accordingly,
        and return a result object with the converted DataFrame, column mappings, and
        original-to-new column name mappings.

        Args:
            df (pd.DataFrame): The input DataFrame.
            infer_dates (bool): If True, attempts to infer datetime columns.
            date_columns (str or list of str): Columns that should always be parsed as dates.
            case (str): The naming convention to apply. Default is "snake".
            truncate_limit (int): The maximum length of the column names. Default is 55.

        Returns:
            SchemaConversionResult: Object containing the DataFrame, dtype map, and column name mapping
        """

        df.replace("", pd.NA, inplace=True)
        df.dropna(axis=1, how="all", inplace=True)

        # Store original column names before cleaning
        original_columns = df.columns.tolist()

        # Clean column names
        cleaned_df = df.copy()
        cleaned_df = SchemaInference.clean_dataframe_names(cleaned_df, case=case, truncate_limit=truncate_limit)

        # Create mapping of original to cleaned column names
        renamed_columns_mapping = dict(zip(original_columns, cleaned_df.columns))

        # Normalize date_columns to a list if provided as a string.
        if isinstance(date_columns, str):
            date_columns = [date_columns]
        elif date_columns is None:
            date_columns = []

        # Determine the schema mapping.
        schema_map = {}
        for col in cleaned_df.columns:
            sql_type, converted_series = SchemaInference.infer_sqlalchemy_type(
                cleaned_df[col], infer_dates=infer_dates or (col in date_columns), date_columns=date_columns
            )
            schema_map[col] = sql_type
            cleaned_df[col] = converted_series

        # Update the DataFrame columns based on the inferred SQLAlchemy types.
        for col, sql_type in schema_map.items():
            if isinstance(sql_type, DateTime):
                # Ensure datetime columns are in datetime64[ns, UTC]
                cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors="coerce", utc=True)
            elif isinstance(sql_type, Integer):
                cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors="coerce").astype("Int64")
            elif isinstance(sql_type, Float):
                cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors="coerce").astype(float)
            elif isinstance(sql_type, Boolean):
                cleaned_df[col] = cleaned_df[col].astype(bool)
            elif isinstance(sql_type, JSON):
                cleaned_df[col] = cleaned_df[col].apply(SchemaInference.safe_json_conversion)
            else:
                cleaned_df[col] = cleaned_df[col].apply(SchemaInference.safe_str_conversion)

        return SchemaConversionResult(
            df=cleaned_df, schema_map=schema_map, renamed_columns_mapping=renamed_columns_mapping
        )

    @staticmethod
    def clean_dataframe_names(df: pd.DataFrame, case: str = "snake", truncate_limit: int = 55) -> pd.DataFrame:
        """
        Clean the column names and index names of a DataFrame using pyjanitors clean_names method.
        Works with both regular Index and MultiIndex.

        Args:
            df (pd.DataFrame): The DataFrame whose column and index names should be cleaned.
            case (str): The naming convention to apply. Default is "snake".
            truncate_limit (int): The maximum length of the column names. Default is 55.

        Returns:
            pd.DataFrame: A new DataFrame with cleaned column and index names.

        Raises:
            ImportError: If pyjanitors is not installed.
        """
        try:
            import janitor  # noqa: F401
        except ImportError as e:
            raise e

        # Clean the column names
        cleaned_df = df.clean_names(case_type=case, truncate_limit=truncate_limit)

        # Handle index names
        if df.index.name is not None:
            # For single indexes with a name
            # Create a temporary dataframe with just the index as a column
            temp_df = pd.DataFrame({df.index.name: df.index})
            cleaned_temp_df = temp_df.clean_names(case_type=case, truncate_limit=truncate_limit)
            # The first and only column name is our cleaned index name
            cleaned_index_name = cleaned_temp_df.columns[0]
            # Set the index name to the cleaned version
            cleaned_df.index.name = cleaned_index_name
        elif hasattr(df.index, "names") and any(name is not None for name in df.index.names):
            # For MultiIndex with at least some named levels
            index_names = df.index.names
            cleaned_index_names = []

            for name in index_names:
                if name is not None:
                    # Create a temporary dataframe with just this index name
                    temp_df = pd.DataFrame({name: [0]})
                    cleaned_temp_df = temp_df.clean_names(case_type=case, truncate_limit=truncate_limit)
                    # The cleaned name is the new column name
                    cleaned_index_names.append(cleaned_temp_df.columns[0])
                else:
                    # Keep None for unnamed levels
                    cleaned_index_names.append(None)

            # Set the new index names
            cleaned_df.index.names = cleaned_index_names

        return cleaned_df


class SchemaConversionResult:
    """Class to hold the results of a DataFrame schema conversion."""

    def __init__(self, df: pd.DataFrame, schema_map: dict[str, Any], renamed_columns_mapping: dict[str, str]):
        """
        Initialize a schema conversion result.

        Args:
            df (pd.DataFrame): The converted DataFrame
            schema_map (dict): Dictionary mapping columns to SQLAlchemy types
            renamed_columns_mapping (dict): Dictionary mapping original column names to new column names
        """
        self.dataframe = df
        self.schema_map = schema_map
        self.renamed_columns_mapping = renamed_columns_mapping

    def __iter__(self) -> Iterator[Any]:
        """
        Make the class iterable to support tuple unpacking.
        Returns the DataFrame, schema_map, and renamed_columns_mapping in that order.
        """
        return iter([self.dataframe, self.schema_map, self.renamed_columns_mapping])
