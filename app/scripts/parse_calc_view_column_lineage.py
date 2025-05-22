"""Extract column-level lineage information from SAP HANA calculation views."""

import json
from typing import Any, Dict, List, Optional, Set

from application_sdk.common.logger_adaptors import get_logger
from app.scripts import utils
from app.scripts.parse_calc_view_lineage import CalcViewLineageExtractor

logger = get_logger(__name__)


def extract_column_lineage_from_calculation_view(
    calc_view: Dict[str, Any], 
    calc_view_columns: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Extract column-level lineage from a calculation view.

    Args:
        calc_view: Dictionary containing calculation view data with ROUTINE_DEFINITION
            containing the calculation view definition as XML/JSON
        calc_view_columns: List of columns for the calculation view

    Returns:
        List[Dict[str, Any]]: List of column lineage items
    """
    column_lineage_items = []
    
    routine_def = calc_view.get("ROUTINE_DEFINITION", "")
    if not routine_def:
        return []
        
    try:
        # Parse the calculation view definition
        parsed_data = None
        if isinstance(routine_def, str):
            parsed_data = json.loads(routine_def)
        else:
            parsed_data = routine_def
            
        # Extract calculation view information
        schema_name = calc_view.get("TABLE_SCHEM", "")
        calc_view_name = calc_view.get("VIEW_NAME", "")
        package_id = calc_view.get("PACKAGE_ID", "")
        
        # Create a lookup dictionary for the calculation view columns
        column_lookup = {}
        for col in calc_view_columns:
            # Only process columns that belong to this calculation view
            if (col.get("VIEW_NAME") == calc_view_name and 
                col.get("TABLE_SCHEM") == schema_name and
                col.get("PACKAGE_ID") == package_id):
                column_lookup[col.get("COLUMN_NAME", "")] = col
        
        # Extract lineage information
        extractor = CalcViewLineageExtractor(parsed_data)
        lineage_items = extractor.extract_lineage()
        
        # Process each lineage item to create column-level lineage
        for item in lineage_items:
            calc_view_column = item.get("calc_view_column", "")
            
            # Skip if column not found in the lookup
            if calc_view_column not in column_lookup:
                continue
                
            # Create column-level lineage
            column_lineage = {
                "calc_view_schema": schema_name,
                "calc_view_name": calc_view_name,
                "calc_view_column": calc_view_column,
                "calc_view_package_id": package_id,
                "source_type": item.get("source_type", ""),
                "source_schema": item.get("source_schema", ""),
                "source_table": item.get("source_table", ""),
                "source_table_column": item.get("source_table_column", ""),
                "source_package_id": item.get("source_package_id", "")
            }
            
            column_lineage_items.append(column_lineage)
            
        return column_lineage_items
    except Exception as e:
        logger.error(f"Error extracting column lineage from calculation view: {e}")
        return []


def process_calculation_view_column_lineage(
    calc_views: List[Dict[str, Any]], 
    calc_view_columns: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Process calculation view data to extract column-level lineage information.
    
    Args:
        calc_views: List of calculation view dictionaries with parsed data
        calc_view_columns: List of calculation view column dictionaries
        
    Returns:
        List[Dict[str, Any]]: List of column-level lineage items
    """
    all_column_lineage = []
    
    for calc_view in calc_views:
        # Skip if there's no routine definition
        if not calc_view.get("ROUTINE_DEFINITION"):
            continue
            
        # Extract column lineage for this calculation view
        lineage_items = extract_column_lineage_from_calculation_view(calc_view, calc_view_columns)
        all_column_lineage.extend(lineage_items)
    
    # Filter out any lineage where source or target information is incomplete
    valid_lineage = []
    for lineage in all_column_lineage:
        if (lineage.get("calc_view_name") and 
            lineage.get("calc_view_column") and
            lineage.get("source_table") and
            lineage.get("source_table_column")):
            valid_lineage.append(lineage)
    
    return valid_lineage 