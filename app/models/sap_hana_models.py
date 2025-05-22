"""
Models for SAP HANA calculation view processing.
"""
from collections.abc import MutableMapping
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple


@dataclass(frozen=True)
class ProcessorInput:
    """
    Input data required for processing calculation views.
    
    Attributes:
        get_calc_views: Function to get calculation views
        get_calc_view_columns: Function to get calculation view columns
        get_schemas: Function to get schemas
        get_tables: Function to get tables
        get_views: Function to get views
        get_tables_views_columns: Function to get all tables and views columns
    """
    get_calc_views: Callable[[], Iterable[Dict[str, Any]]] = lambda: []
    get_calc_view_columns: Callable[[], Iterable[Dict[str, Any]]] = lambda: []
    get_schemas: Callable[[], Iterable[Dict[str, Any]]] = lambda: []
    get_tables: Callable[[], Iterable[Dict[str, Any]]] = lambda: []
    get_views: Callable[[], Iterable[Dict[str, Any]]] = lambda: []
    get_tables_views_columns: Callable[[], Iterable[Dict[str, Any]]] = lambda: []


@dataclass(frozen=True)
class MetadataHolder:
    """
    Holder for metadata maps used during processing.
    
    Attributes:
        create_column_ordinal_map: Function to create the column ordinals map
        create_table_view_map: Function to create the table/view map
        create_calc_view_map: Function to create the calculation view map
        create_calc_view_column_map: Function to create the calculation view column map
        create_tables_views_column_map: Function to create the tables/views column map
        create_calc_view_schema_map: Function to create the calculation view schema map
        create_processes_map: Function to create the processes map
    """
    create_column_ordinal_map: Callable[[], MutableMapping[str, Dict[str, Any]]] = lambda: {}
    create_table_view_map: Callable[[], MutableMapping[str, bool]] = lambda: {}
    create_calc_view_map: Callable[[], MutableMapping[str, bool]] = lambda: {}
    create_calc_view_column_map: Callable[[], MutableMapping[str, bool]] = lambda: {}
    create_tables_views_column_map: Callable[[], MutableMapping[str, bool]] = lambda: {}
    create_calc_view_schema_map: Callable[[], MutableMapping[str, str]] = lambda: {}
    create_processes_map: Callable[[], MutableMapping[str, bool]] = lambda: {} 