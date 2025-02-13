import logging
import warnings
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, ClassVar, Optional, Union

import pandas as pd
import yaml  # type: ignore
from sqlalchemy import Boolean, DateTime, Float, Integer, Text
from sqlalchemy.dialects.postgresql import JSON

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class SchemaInference:
    """
    A class for inferring SQLAlchemy types from Pandas DataFrame columns and handling schema I/O operations.
    """

    SQLALCHEMY_TYPE_MAP: ClassVar[dict[str, Any]] = {
        "TIMESTAMP(timezone=True)": DateTime(timezone=True),
        "INTEGER": Integer(),
        "FLOAT": Float(),
        "TEXT": Text(),
        "JSON": JSON,
        "BOOLEAN": Boolean(),
    }

    @staticmethod
    def save_schema_to_yaml(dtype_map: dict[str, Any], filename: Union[str, Path], schema_name: str) -> None:
        """
        Save dtype_map to a YAML file, storing SQLAlchemy types as text strings.
        The YAML content will be nested under the provided schema_name key and further under the 'columns' key.

        Args:
            dtype_map (dict): Dictionary mapping column names to SQLAlchemy types.
            filename (str or Path): Path to the YAML file.
            schema_name (str): The parent key to use in the YAML file (required).
        """
        dtype_map_serializable: dict[str, str] = {
            col: "TEXT" if isinstance(sql_type, Text) else str(sql_type) for col, sql_type in dtype_map.items()
        }

        content = {schema_name: {"columns": dtype_map_serializable}}

        with open(filename, "w", encoding="utf-8") as file:
            yaml.dump(content, file, sort_keys=False)

    @staticmethod
    def load_schema_from_yaml(filename: Union[str, Path], schema_name: str) -> dict[str, Any]:
        """
        Load schema from a YAML file and convert stored text strings back into SQLAlchemy types.
        The method looks for the schema under the provided schema_name key and within its 'columns' mapping.

        Args:
            filename (str or Path): Path to the YAML file.
            schema_name (str): The parent key under which the schema is stored (required).

        Returns:
            dict: Dictionary mapping column names to SQLAlchemy types.

        Raises:
            KeyError: If the provided schema_name or the 'columns' key is not found in the YAML file.
        """
        with open(filename, encoding="utf-8") as file:
            loaded_content: dict[str, Any] = yaml.safe_load(file) or {}

        loaded_schema = loaded_content.get(schema_name)
        if loaded_schema is None:
            raise KeyError(f"Schema '{schema_name}' not found in {filename}")

        columns_mapping = loaded_schema.get("columns")
        if columns_mapping is None:
            raise KeyError(f"'columns' key not found under schema '{schema_name}' in {filename}")

        return {
            col: SchemaInference.SQLALCHEMY_TYPE_MAP.get(sql_type or "TEXT", Text())
            for col, sql_type in columns_mapping.items()
        }

    @staticmethod
    def detect_and_convert_datetime(series: pd.Series) -> tuple[pd.Series, bool]:
        """
        Detects datetime format in a Pandas Series and converts it to datetime64[ns, UTC].
        Supports:
        - ISO 8601 formats (YYYY-MM-DDTHH:MM:SS.sssZ)
        - RFC 2822 (email/HTTP format)
        Returns the converted series and a boolean indicating success.
        """
        # Use only non-null values for detection.
        non_null = series.dropna()
        if non_null.empty:
            return pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns, UTC]"), False

        try:
            # Attempt ISO 8601 conversion
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                parsed_dates = pd.to_datetime(series, errors="coerce", utc=True)
            # Check only the non-null values.
            if parsed_dates[series.notna()].notna().all():
                return parsed_dates, True
        except Exception as e:
            logger.debug(f"Error parsing ISO 8601 datetime format: {e}")

        try:
            # Attempt RFC 2822 conversion
            parsed_dates = series.apply(
                lambda x: parsedate_to_datetime(x).astimezone(pd.Timestamp.utc)
                if pd.notnull(x) and isinstance(x, str)
                else pd.NaT
            )
            if parsed_dates[series.notna()].notna().all():
                return parsed_dates, True
        except Exception as e:
            logger.debug(f"Error parsing RFC 2822 datetime format: {e}")

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
        non_null = series.dropna()
        if non_null.empty:
            return Text(), series.astype(str)

        sample = non_null.iloc[0]

        # --- JSON-like objects ---
        if isinstance(sample, (dict, list)):
            return JSON(), series

        # --- Boolean conversion ---
        if pd.api.types.is_bool_dtype(series):
            return Boolean(), series

        unique_vals = set(non_null.dropna().unique())
        if unique_vals.issubset({0, 1}):
            return Boolean(), series.astype(bool)

        true_vals = {"true", "t", "yes", "y", "1"}
        false_vals = {"false", "f", "no", "n", "0"}
        lower_non_null = non_null.astype(str).str.lower().str.strip()

        if lower_non_null.isin(true_vals.union(false_vals)).all():
            converted = series.astype(str).str.lower().str.strip().map(lambda x: x in true_vals)
            return Boolean(), converted

        # --- Numeric conversion ---
        try:
            numeric_series = pd.to_numeric(series, errors="raise")
            if numeric_series.dropna().apply(lambda x: float(x).is_integer()).all():
                return Integer(), numeric_series.astype("Int64")
            return Float(), numeric_series.astype(float)
        except Exception as e:
            logger.debug(f"Error parsing numeric values: {e}")

        # --- DateTime Conversion ---
        if infer_dates or (date_columns and series.name in date_columns):
            converted_series, is_datetime = SchemaInference.detect_and_convert_datetime(series)
            if is_datetime:
                return DateTime(timezone=True), converted_series

        # --- Fallback to text ---
        return Text(), series.astype(str)

    @staticmethod
    def sync_schema(
        df: pd.DataFrame,
        schema_file: Union[str, Path],
        sync_method: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Synchronizes a schema file with the current DataFrame. The schema is nested under the provided
        schema_name key in the YAML file, with the columns stored under the 'columns' key.

        Args:
            df (pd.DataFrame): The input DataFrame.
            schema_file (str or Path): Path to an existing schema file.
            sync_method (str): Either "update" or "overwrite". If provided without schema_file, raises an error.
            schema_name (str): The parent key to use for the schema in the YAML file (required).

        Returns:
            dict: The updated schema mapping column names to SQLAlchemy types.
        """
        if sync_method and not schema_file:
            raise ValueError("sync_method cannot be used without specifying schema_file.")

        existing_schema = {}
        schema_path = Path(schema_file)
        if schema_path.exists():
            if schema_name is not None:
                existing_schema = SchemaInference.load_schema_from_yaml(schema_path, schema_name)
            else:
                raise ValueError("schema_name cannot be None")

        # If updating, only process columns not already in the schema; else process all columns.
        new_columns = df.columns
        if sync_method == "update" and existing_schema:
            new_columns = [col for col in df.columns if col not in existing_schema]

        dtype_map = {}
        for col in new_columns:
            sql_type, _ = SchemaInference.infer_sqlalchemy_type(df[col])
            dtype_map[col] = sql_type

        if sync_method == "update" and existing_schema:
            dtype_map = {**existing_schema, **dtype_map}

        if schema_name is not None:
            SchemaInference.save_schema_to_yaml(dtype_map, schema_file, schema_name)
        else:
            raise ValueError("schema_name cannot be None")

        return dtype_map

    @staticmethod
    def convert_dataframe(
        df: pd.DataFrame,
        infer_dates: bool = True,
        schema_name: Optional[str] = None,
        date_columns: Optional[Union[str, list[str]]] = None,
        schema_file: Optional[Union[str, Path]] = None,
        sync_method: Optional[str] = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """
        Infer the SQLAlchemy type for each column, convert the DataFrame accordingly,
        and optionally synchronize the schema with a YAML file. The schema in the YAML file
        will be nested under the provided schema_name key, with the column mapping stored under 'columns'.

        Args:
            df (pd.DataFrame): The input DataFrame.
            infer_dates (bool): If True, attempts to infer datetime columns.
            date_columns (str or list of str): Columns that should always be parsed as dates.
            schema_file (str or Path): Path to an existing schema file.
            sync_method (str): Either "update" or "overwrite". If provided without schema_file, raises an error.
            schema_name (str): The parent key to use for the schema in the YAML file (required).

        Returns:
            tuple: (updated DataFrame, dictionary mapping columns to SQLAlchemy types)
        """
        df = df.copy()

        # Normalize date_columns to a list if provided as a string.
        if isinstance(date_columns, str):
            date_columns = [date_columns]
        elif date_columns is None:
            date_columns = []

        # Determine the schema mapping.
        if schema_file:
            dtype_map = SchemaInference.sync_schema(df, schema_file, sync_method, schema_name)
        else:
            dtype_map = {}
            for col in df.columns:
                sql_type, converted_series = SchemaInference.infer_sqlalchemy_type(
                    df[col], infer_dates=infer_dates or (col in date_columns), date_columns=date_columns
                )
                dtype_map[col] = sql_type
                df[col] = converted_series

        # Update the DataFrame columns based on the inferred SQLAlchemy types.
        for col, sql_type in dtype_map.items():
            if isinstance(sql_type, DateTime):
                # Ensure datetime columns are in datetime64[ns, UTC]
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
                except Exception as e:
                    logger.debug(f"Error converting column {col} to datetime: {e}")
            elif isinstance(sql_type, Integer):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif isinstance(sql_type, Float):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
            elif isinstance(sql_type, Boolean):
                df[col] = df[col].astype(bool)
            else:
                df[col] = df[col].astype(str)

        return df, dtype_map

    @staticmethod
    def clean_dataframe_names(df: pd.DataFrame, case: str = "snake", truncate: int = 55) -> pd.DataFrame:
        """
        Clean the column names of a DataFrame using pyjanitors' clean_names method.

        Args:
            df (pd.DataFrame): The DataFrame whose column names should be cleaned.
            case (str): The naming convention to apply. Default is "snake".
            truncate (int): The maximum length of the column names. Default is 55.

        Returns:
            pd.DataFrame: A new DataFrame with cleaned column names.

        Raises:
            ImportError: If pyjanitors is not installed.
        """
        try:
            import janitor  # noqa: F401
        except ImportError as e:
            logger.error("pyjanitors is required for cleaning names. Please install it using 'pip install pyjanitor'")
            raise e

        return df.clean_names(case=case, truncate=truncate)
