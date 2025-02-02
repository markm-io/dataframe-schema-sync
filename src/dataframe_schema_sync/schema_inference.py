import logging
import warnings
from email.utils import parsedate_to_datetime
from typing import Any, ClassVar, Optional, Union

import pandas as pd
from sqlalchemy import Boolean, DateTime, Float, Integer, Text
from sqlalchemy.dialects.postgresql import JSON

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class SchemaInference:
    """
    A class for inferring SQLAlchemy types from Pandas DataFrame columns.
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
    def detect_and_convert_datetime(series: pd.Series) -> tuple[pd.Series, bool]:
        """
        Detects datetime format in a Pandas Series and converts it to datetime64[ns, UTC].
        Supports:
        - ISO 8601 formats (YYYY-MM-DDTHH:MM:SS.sssZ)
        - RFC 2822 (email/HTTP format)
        """
        non_null = series.dropna()

        if non_null.empty:
            return pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns, UTC]"), False

        try:
            # --- Direct ISO 8601 or standard datetime formats ---
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                parsed_dates = pd.to_datetime(series, errors="coerce", utc=True)
            if parsed_dates.notna().any():
                return parsed_dates, True
        except Exception as e:
            logger.debug(f"Error parsing ISO 8601 datetime format: {e}")

        # --- RFC 2822 Format (Emails, HTTP headers) ---
        try:
            parsed_dates = series.apply(
                lambda x: parsedate_to_datetime(x).astimezone(pd.Timestamp.utc)
                if pd.notnull(x) and isinstance(x, str)
                else pd.NaT
            )
            return parsed_dates, True
        except Exception as e:
            logger.debug(f"Error parsing RFC 2822 datetime format: {e}")

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
            converted = series.apply(lambda x: bool(x) if pd.notnull(x) else x)
            return Boolean(), converted

        true_vals = {"true", "t", "yes", "y", "1"}
        false_vals = {"false", "f", "no", "n", "0"}
        lower_non_null = non_null.astype(str).str.lower().str.strip()

        if lower_non_null.isin(true_vals.union(false_vals)).all():

            def parse_bool(val: str) -> Optional[bool]:
                if val in true_vals:
                    return True
                if val in false_vals:
                    return False
                return None

            converted = series.astype(str).str.lower().str.strip().map(parse_bool)
            return Boolean(), converted

        # --- Ensure mixed types default to Text ---
        if (
            series.dropna().apply(lambda x: isinstance(x, (int, float))).any()
            and series.dropna().apply(lambda x: isinstance(x, str)).any()
        ):
            return Text(), series.astype(str)  # Convert all values to string

        # --- Numeric conversion (ensure integers are detected first) ---
        if pd.api.types.is_numeric_dtype(series):
            try:
                numeric_series = pd.to_numeric(series, errors="raise")

                # If values are all effectively integers, treat as Integer
                if numeric_series.dropna().apply(lambda x: float(x).is_integer()).all():
                    return Integer(), numeric_series.astype("Int64")

                return Float(), numeric_series.astype(float)
            except Exception as e:
                logger.debug(f"Error parsing numeric values: {e}")

        # --- DateTime Conversion (only check after confirming it's not an integer) ---
        if infer_dates or (date_columns and series.name in date_columns):
            converted_series, is_datetime = SchemaInference.detect_and_convert_datetime(series)
            if is_datetime:
                return DateTime(timezone=True), converted_series

        # --- Fallback to text ---
        return Text(), series.astype(str)

    @staticmethod
    def convert_dataframe(
        df: pd.DataFrame, infer_dates: bool = True, date_columns: Optional[Union[str, list[str]]] = None
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """
        Infer the SQLAlchemy type for each column and convert the DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame.
            infer_dates (bool): If True, attempts to infer datetime columns.
            date_columns (str or list of str): Specifies columns that should always be parsed as dates.

        Returns:
            tuple: (updated DataFrame, dictionary mapping columns to SQLAlchemy types)
        """
        df = df.copy()
        dtype_map: dict[str, Any] = {}

        if isinstance(date_columns, str):
            date_columns = [date_columns]
        elif not date_columns:
            date_columns = []

        for col in df.columns:
            should_parse_as_date = col in date_columns
            sql_type, converted_series = SchemaInference.infer_sqlalchemy_type(
                df[col], infer_dates=infer_dates or should_parse_as_date
            )
            dtype_map[col] = sql_type

            if isinstance(sql_type, DateTime):
                df[col] = converted_series.astype("datetime64[ns, UTC]")
            elif isinstance(sql_type, Integer):
                df[col] = pd.to_numeric(converted_series, errors="coerce").astype("Int64")
            elif isinstance(sql_type, Float):
                df[col] = pd.to_numeric(converted_series, errors="coerce").astype(float)
            elif isinstance(sql_type, Boolean):
                df[col] = converted_series.astype(bool)
            else:
                df[col] = converted_series.astype(str)

        return df, dtype_map
