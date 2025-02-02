from typing import Any, Union

import yaml  # type: ignore
from sqlalchemy import Text

from .schema_inference import SchemaInference


class SchemaIO:
    """
    A class for saving and loading DataFrame schema definitions using YAML.
    """

    @staticmethod
    def save_schema_to_yaml(dtype_map: dict[str, Any], filename: str = "schema.yaml") -> None:
        """
        Save dtype_map to a YAML file, storing SQLAlchemy types as text strings.

        Args:
            dtype_map (dict): Dictionary mapping column names to SQLAlchemy types.
            filename (str): Path to the YAML file.
        """
        dtype_map_serializable: dict[str, str] = {
            col: "TEXT" if isinstance(sql_type, Text) else str(sql_type) for col, sql_type in dtype_map.items()
        }

        with open(filename, "w", encoding="utf-8") as file:
            yaml.dump(dtype_map_serializable, file, sort_keys=False)

    @staticmethod
    def load_schema_from_yaml(filename: str = "schema.yaml") -> dict[str, Any]:
        """
        Load schema from a YAML file and convert stored text strings back into SQLAlchemy types.

        Args:
            filename (str): Path to the YAML file.

        Returns:
            dict: Dictionary mapping column names to SQLAlchemy types.
        """
        with open(filename, encoding="utf-8") as file:
            loaded_schema: dict[str, Union[str, None]] = yaml.safe_load(file) or {}

        return {
            col: SchemaInference.SQLALCHEMY_TYPE_MAP.get(sql_type or "TEXT", Text())
            for col, sql_type in loaded_schema.items()
        }
