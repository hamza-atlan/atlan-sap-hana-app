"""Utility functions for SAP HANA calculation view processing."""

from typing import Any, Dict, List, Optional, Tuple, Union, Iterable
import json
import logging
import re

from application_sdk.common.logger_adaptors import get_logger

logger = get_logger(__name__)


def convert_to_list(obj: Any) -> List[Any]:
    """Convert any object to a list.

    If the object is None, returns an empty list.
    If the object is a list, returns the object.
    Otherwise, returns a list containing the object.

    Args:
        obj: Any object to convert to a list

    Returns:
        List[Any]: The converted list
    """
    if obj is None:
        return []
    elif isinstance(obj, list):
        return obj
    else:
        return [obj]


def convert_iterable_back_to_original_form(original_form: Any, converted_list: List[Any]) -> Any:
    """Convert a list back to its original form.

    If the original form is None, returns None.
    If the original form is a list, returns the converted list.
    Otherwise, returns the first element of the converted list if it exists, or None.

    Args:
        original_form: The original form of the object
        converted_list: The converted list to convert back

    Returns:
        Any: The converted object
    """
    if original_form is None:
        return None
    elif isinstance(original_form, list):
        return converted_list
    else:
        return converted_list[0] if converted_list else None


def group_by_first_element(data: List[Tuple[Any, Any]]) -> Dict[Any, List[Any]]:
    """Group data by the first element of each tuple.

    Args:
        data: List of tuples where the first element will be used as a key for grouping

    Returns:
        Dict[Any, List[Any]]: Dictionary where keys are the first elements of the tuples and values are lists
            of all second elements that shared that first element
    """
    result: Dict[Any, List[Any]] = {}
    for key, value in data:
        if key not in result:
            result[key] = []
        result[key].append(value)
    return result


def is_column_valid(column: Dict[str, Any]) -> bool:
    """Check if a calculation view column is valid.

    A column is valid if it has an ID.

    Args:
        column: Dictionary representing a calculation view column

    Returns:
        bool: True if the column is valid, False otherwise
    """
    return column and "@id" in column


def extract_package_from_resourceuri(resource_uri: Optional[str]) -> str:
    """Extract package ID from a resource URI.

    Args:
        resource_uri: Resource URI containing the package ID

    Returns:
        str: The extracted package ID or an empty string if not found
    """
    if not resource_uri:
        return ""

    match = re.search(r"/([^/]+)/", resource_uri)
    if match:
        return match.group(1)
    return ""


def get_valid_table_keys(
    schema_name_key: str, 
    table_name_key: str, 
    table_iterable: Iterable[Dict[str, Any]]
) -> List[str]:
    """Get valid table keys from an iterable of tables.

    Args:
        schema_name_key: Key for the schema name in the table dictionary
        table_name_key: Key for the table name in the table dictionary
        table_iterable: Iterable of table dictionaries

    Returns:
        List[str]: List of valid table keys in the format "schema_name.table_name"
    """
    table_keys = []
    for table in table_iterable:
        schema_name = table.get(schema_name_key)
        table_name = table.get(table_name_key)
        if schema_name and table_name:
            table_key = f"{schema_name}.{table_name}"
            table_keys.append(table_key)
    return table_keys


def get_valid_table_view_column_keys(
    schema_name_key: str,
    table_name_key: str,
    column_name_key: str,
    table_view_column_iterable: Iterable[Dict[str, Any]]
) -> List[str]:
    """Get valid table/view column keys from an iterable of columns.

    Args:
        schema_name_key: Key for the schema name in the column dictionary
        table_name_key: Key for the table name in the column dictionary
        column_name_key: Key for the column name in the column dictionary
        table_view_column_iterable: Iterable of column dictionaries

    Returns:
        List[str]: List of valid column keys in the format "schema_name.table_name.column_name"
    """
    column_keys = []
    for column in table_view_column_iterable:
        schema_name = column.get(schema_name_key)
        table_name = column.get(table_name_key)
        column_name = column.get(column_name_key)
        if schema_name and table_name and column_name:
            column_key = f"{schema_name}.{table_name}.{column_name}"
            column_keys.append(column_key)
    return column_keys


def get_valid_calc_view_keys(
    schema_name_key: str,
    package_id_key: str,
    calc_view_name_key: str,
    calc_view_iterable: Iterable[Dict[str, Any]]
) -> List[str]:
    """Get valid calculation view keys from an iterable of calculation views.

    Args:
        schema_name_key: Key for the schema name in the calculation view dictionary
        package_id_key: Key for the package ID in the calculation view dictionary
        calc_view_name_key: Key for the calculation view name in the dictionary
        calc_view_iterable: Iterable of calculation view dictionaries

    Returns:
        List[str]: List of valid calculation view keys in the format "schema_name.package_id.calc_view_name"
    """
    calc_view_keys = []
    for calc_view in calc_view_iterable:
        schema_name = calc_view.get(schema_name_key)
        package_id = calc_view.get(package_id_key)
        calc_view_name = calc_view.get(calc_view_name_key)
        if schema_name and package_id and calc_view_name:
            calc_view_key = f"{schema_name}.{package_id}.{calc_view_name}"
            calc_view_keys.append(calc_view_key)
    return calc_view_keys


def get_valid_calc_view_column_keys(
    schema_name_key: str,
    package_id_key: str,
    calc_view_name_key: str,
    column_name_key: str,
    calc_view_column_iterable: Iterable[Dict[str, Any]]
) -> List[str]:
    """Get valid calculation view column keys from an iterable of calculation view columns.

    Args:
        schema_name_key: Key for the schema name in the calculation view column dictionary
        package_id_key: Key for the package ID in the calculation view column dictionary
        calc_view_name_key: Key for the calculation view name in the dictionary
        column_name_key: Key for the column name in the calculation view column dictionary
        calc_view_column_iterable: Iterable of calculation view column dictionaries

    Returns:
        List[str]: List of valid calculation view column keys in the format 
            "schema_name.package_id.calc_view_name.column_name"
    """
    column_keys = []
    for column in calc_view_column_iterable:
        schema_name = column.get(schema_name_key)
        package_id = column.get(package_id_key)
        calc_view_name = column.get(calc_view_name_key)
        column_name = column.get(column_name_key)
        if schema_name and package_id and calc_view_name and column_name:
            column_key = f"{schema_name}.{package_id}.{calc_view_name}.{column_name}"
            column_keys.append(column_key)
    return column_keys 