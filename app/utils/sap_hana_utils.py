"""
Utility functions for SAP HANA calculation view processing.
"""
import ast
import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import orjson
import xmltodict

from application_sdk.common.logger_adaptors import get_logger

logger = get_logger(__name__)


def parse_xml(xml_input: Optional[str]) -> Dict[str, Any]:
    """
    Parse XML string into a dictionary.
    
    Args:
        xml_input: XML string to parse
    
    Returns:
        Dict[str, Any]: Parsed XML as dictionary
    """
    if not xml_input:
        return {}
    try:
        json_object = xmltodict.parse(xml_input)
        return orjson.loads(orjson.dumps(json_object))
    except Exception as e:
        logger.debug(f"Error during XML to JSON conversion: {e}")
        return {}


def convert_to_list(data: Optional[Any]) -> List[Any]:
    """
    Convert data to a list if it's not already.
    
    Args:
        data: Input data which might be a dict, list, or other
    
    Returns:
        List[Any]: The data as a list
    """
    if isinstance(data, dict):
        return [data]
    elif isinstance(data, list):
        return data
    else:
        return []


def convert_iterable_back_to_original_form(data: List[Any]) -> Union[Dict, List, None]:
    """
    Convert a list back to its original form.
    
    If the list contains a single dictionary, returns the dictionary.
    Otherwise, returns the list as is.
    
    Args:
        data: List to convert
    
    Returns:
        Union[Dict, List, None]: Converted data
    """
    if len(data) == 1 and isinstance(data[0], dict):
        return data[0]  # Convert back to dictionary if the list contains a single dictionary
    elif isinstance(data, list) and len(data) > 0:
        return data  # If it's a list, just return it as is
    else:
        return {}


def group_by_first_element(main_dict: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Group dictionary by the first element of tuple keys.
    
    Args:
        main_dict: Dictionary with tuple keys as strings
    
    Returns:
        Dict[str, Dict[str, Any]]: Grouped dictionary
    """
    grouped_dict = defaultdict(dict)

    for key, value in main_dict.items():
        # Evaluate the key string into a tuple
        key_tuple = ast.literal_eval(key)
        # Group by the first element of the tuple
        grouped_dict[key_tuple[0]][key_tuple[1]] = value

    return dict(grouped_dict)


def is_column_valid(column: Dict[str, Any]) -> bool:
    """
    Check if a column is valid.
    
    A column is considered valid if it has an id and 
    doesn't contain the $ character.
    
    Args:
        column: Column dictionary
    
    Returns:
        bool: True if the column is valid, False otherwise
    """
    return column.get("@id") and "$" not in column.get("@id")


def get_valid_table_keys(
    schema_name_key: str, 
    table_name_key: str, 
    table_iterable: Iterable[Dict[str, Any]]
) -> Iterable[str]:
    """
    Get valid table keys from table iterables.
    
    Args:
        schema_name_key: Key for schema name in table dictionaries
        table_name_key: Key for table name in table dictionaries
        table_iterable: Iterable of table dictionaries
    
    Returns:
        Iterable[str]: Valid table keys
    """
    return filter(
        lambda key: key is not None,
        map(
            lambda table: get_table_key(
                table.get(schema_name_key),
                table.get(table_name_key)
            ),
            table_iterable,
        ),
    )


def get_table_key(
    schema_name: Optional[str],
    table_name: Optional[str]
) -> Optional[str]:
    """
    Get table key from schema and table names.
    
    Args:
        schema_name: Schema name
        table_name: Table name
    
    Returns:
        Optional[str]: Table key or None if inputs are invalid
    """
    if not schema_name or not table_name:
        return None
    return str((schema_name, table_name))


def get_valid_table_view_column_keys(
    schema_name_key: str,
    table_name_key: str,
    column_name_key: str,
    table_view_column_iterable: Iterable[Dict[str, Any]],
) -> Iterable[str]:
    """
    Get valid table/view column keys.
    
    Args:
        schema_name_key: Key for schema name in column dictionaries
        table_name_key: Key for table name in column dictionaries
        column_name_key: Key for column name in column dictionaries
        table_view_column_iterable: Iterable of column dictionaries
    
    Returns:
        Iterable[str]: Valid column keys
    """
    return filter(
        lambda key: key is not None,
        map(
            lambda table: get_table_view_column_key(
                table.get(schema_name_key),
                table.get(table_name_key),
                table.get(column_name_key),
            ),
            table_view_column_iterable,
        ),
    )


def get_table_view_column_key(
    schema_name: Optional[str], 
    table_name: Optional[str], 
    column_name: Optional[str]
) -> Optional[str]:
    """
    Get table/view column key from schema, table, and column names.
    
    Args:
        schema_name: Schema name
        table_name: Table name
        column_name: Column name
    
    Returns:
        Optional[str]: Column key or None if inputs are invalid
    """
    if not schema_name or not table_name or not column_name:
        return None
    return str((schema_name, table_name, column_name))


def get_valid_calc_view_keys(
    schema_name_key: str,
    package_id_key: str,
    calc_view_name_key: str,
    calc_view_iterable: Iterable[Dict[str, Any]],
) -> Iterable[str]:
    """
    Get valid calculation view keys.
    
    Args:
        schema_name_key: Key for schema name in calculation view dictionaries
        package_id_key: Key for package ID in calculation view dictionaries
        calc_view_name_key: Key for calculation view name in calculation view dictionaries
        calc_view_iterable: Iterable of calculation view dictionaries
    
    Returns:
        Iterable[str]: Valid calculation view keys
    """
    return filter(
        lambda key: key is not None,
        map(
            lambda table: get_calc_view_key(
                table.get(schema_name_key),
                table.get(package_id_key),
                table.get(calc_view_name_key),
            ),
            calc_view_iterable,
        ),
    )


def get_calc_view_key(
    schema_name: Optional[str],
    package_name: Optional[str],
    calc_view_name_key: Optional[str],
) -> Optional[str]:
    """
    Get calculation view key from schema, package, and view names.
    
    Args:
        schema_name: Schema name
        package_name: Package name
        calc_view_name_key: Calculation view name
    
    Returns:
        Optional[str]: Calculation view key or None if inputs are invalid
    """
    if not schema_name or not calc_view_name_key or not package_name:
        return None
    return str((schema_name, package_name, calc_view_name_key))


def get_valid_calc_view_column_keys(
    schema_name_key: str,
    package_id_key: str,
    calc_view_name_key: str,
    column_name_key: str,
    calc_view_column_iterable: Iterable[Dict[str, Any]],
) -> Iterable[str]:
    """
    Get valid calculation view column keys.
    
    Args:
        schema_name_key: Key for schema name in column dictionaries
        package_id_key: Key for package ID in column dictionaries
        calc_view_name_key: Key for calculation view name in column dictionaries
        column_name_key: Key for column name in column dictionaries
        calc_view_column_iterable: Iterable of column dictionaries
    
    Returns:
        Iterable[str]: Valid calculation view column keys
    """
    return filter(
        lambda key: key is not None,
        map(
            lambda table: get_calc_column_key(
                table.get(schema_name_key),
                table.get(package_id_key),
                table.get(calc_view_name_key),
                table.get(column_name_key),
            ),
            calc_view_column_iterable,
        ),
    )


def get_calc_column_key(
    schema_name: Optional[str],
    package_name: Optional[str],
    calc_view_name_key: Optional[str],
    column_name: Optional[str],
) -> Optional[str]:
    """
    Get calculation view column key from schema, package, view, and column names.
    
    Args:
        schema_name: Schema name
        package_name: Package name
        calc_view_name_key: Calculation view name
        column_name: Column name
    
    Returns:
        Optional[str]: Column key or None if inputs are invalid
    """
    if not schema_name or not calc_view_name_key or not package_name or not column_name:
        return None
    return str((schema_name, package_name, calc_view_name_key, column_name))


def normalize_data_type(data_type: Optional[str]) -> str:
    """
    Normalize SQL data types to a standard format.
    
    Args:
        data_type: SQL data type
    
    Returns:
        str: Normalized data type
    """
    varchar_pattern = r"^VARCHAR(\(.*\))?$"
    nvarchar_pattern = r"^NVARCHAR(\(.*\))?$"
    decimal_pattern = r"^DECIMAL(\(.*\))?$"
    
    if not data_type or data_type.strip() == "":
        return "null"
    if re.match(varchar_pattern, data_type):
        return "VARCHAR"
    if re.match(nvarchar_pattern, data_type):
        return "NVARCHAR"
    if re.match(decimal_pattern, data_type):
        return "DECIMAL"
    return data_type


def extract_package_from_resourceuri(input_string: Optional[str]) -> str:
    """
    Extract package ID from SAP HANA resource URI.
    
    Args:
        input_string: Resource URI string
    
    Returns:
        str: Package ID or empty string if not found
    """
    if not input_string:
        return ""
        
    pattern = r"/([^/]+)/[^/]+\.calculationview"
    match = re.search(pattern, input_string)
    
    if match:
        return match.group(1)
    return ""


def get_package_id_for_source_calc_view(source: Dict[str, Any]) -> str:
    """
    Get package ID for a source calculation view.
    
    Args:
        source: Source dictionary
    
    Returns:
        str: Package ID or empty string if not found
    """
    if not source:
        return ""
    
    resource_uri = source.get("resourceUri", "")
    return extract_package_from_resourceuri(resource_uri)


def get_data_sources(calc_view: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Get data sources from a calculation view.
    
    Args:
        calc_view: Calculation view dictionary
    
    Returns:
        List[Dict[str, Any]]: List of data sources
    """
    parsed_data = calc_view.get("PARSED_DATA", {})
    calc_scenario = parsed_data.get("Calculation:scenario", {})
    data_sources = calc_scenario.get("dataSources", {})
    
    if not data_sources:
        return []
        
    data_source_list = data_sources.get("DataSource", [])
    return convert_to_list(data_source_list)


def generate_process_key(schema: str, package: str, table_or_view: str) -> str:
    """
    Generate a process key for a calculation view.
    
    Args:
        schema: Schema name
        package: Package ID
        table_or_view: Table or view name
    
    Returns:
        str: Process key
    """
    return f"{schema}/{package}/{table_or_view}"


def generate_column_key(
    schema_name: str, 
    package_name: str, 
    table_name: str, 
    column_name: str
) -> str:
    """
    Generate a key for a calculation view column.
    
    Args:
        schema_name: Schema name
        package_name: Package ID
        table_name: Table or view name
        column_name: Column name
    
    Returns:
        str: Column key
    """
    return f"{schema_name}/{package_name}/{table_name}/{column_name}" 