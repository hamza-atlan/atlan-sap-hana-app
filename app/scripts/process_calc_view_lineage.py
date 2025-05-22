"""
Process lineage information from SAP HANA calculation views.

This script analyzes the XML content of SAP HANA calculation views
to extract lineage information between source tables/views and the 
calculation view itself.
"""
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Optional
import json

from application_sdk.common.logger_adaptors import get_logger

logger = get_logger(__name__)

def extract_source_objects(xml_content: str) -> List[Dict[str, str]]:
    """
    Extract source tables and views from calculation view XML content.
    
    Args:
        xml_content: XML definition of the calculation view
        
    Returns:
        List of dictionaries containing source object information
    """
    try:
        # Parse the XML
        root = ET.fromstring(xml_content)
        
        # Find all data source nodes
        data_sources = []
        
        # Look for various types of data sources in the XML
        # Different calculation view types have different XML structures
        
        # Type 1: Standard data source nodes
        for datasource in root.findall(".//dataSources/DataSource"):
            source_type = datasource.get("type", "TABLE")
            schema_name = datasource.get("schemaName", "")
            table_name = datasource.get("objectName", "")
            
            if schema_name and table_name:
                source_type_mapped = map_source_type(source_type)
                data_sources.append({
                    "schema": schema_name,
                    "name": table_name,
                    "type": source_type_mapped
                })
        
        # Type 2: Look for columnView references
        for columnview in root.findall(".//columnView"):
            for input_node in columnview.findall(".//input/viewAttributes"):
                ref = input_node.get("columnViewReference", "")
                if ref:
                    parts = ref.split(".")
                    if len(parts) >= 2:
                        schema_name = parts[0]
                        view_name = parts[1]
                        data_sources.append({
                            "schema": schema_name,
                            "name": view_name,
                            "type": "CalculationView"
                        })
                        
        # Type 3: Check for tableType datasources
        for table_type in root.findall(".//tableType"):
            schema_name = table_type.get("schemaName", "")
            table_name = table_type.get("columnObjectName", "")
            if schema_name and table_name:
                data_sources.append({
                    "schema": schema_name,
                    "name": table_name,
                    "type": "Table"
                })
                
        return data_sources
        
    except Exception as e:
        logger.error(f"Error extracting source objects from calculation view XML: {str(e)}")
        return []

def map_source_type(source_type: str) -> str:
    """
    Map SAP HANA source types to standard entity types.
    
    Args:
        source_type: Source type from XML
        
    Returns:
        Mapped type name
    """
    type_mapping = {
        "TABLE": "Table",
        "VIEW": "View",
        "CALC_VIEW": "CalculationView",
        "CALCULATION_VIEW": "CalculationView",
        "ANALYTIC_VIEW": "View"
    }
    
    return type_mapping.get(source_type.upper(), "Table")

def process_calc_view_lineage(calc_views: List[Dict[str, Any]], connection_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Process calculation view definitions to extract lineage information.
    
    Args:
        calc_views: List of calculation view data with XML content
        connection_info: Connection metadata with qualified name
        
    Returns:
        List of Process entities representing lineage relationships
    """
    lineage_entities = []
    connection_qn = connection_info.get("connection_qualified_name", "")
    
    for calc_view in calc_views:
        view_name = calc_view.get("VIEW_NAME", "")
        schema_name = calc_view.get("TABLE_SCHEM", "")
        xml_content = calc_view.get("ROUTINE_DEFINITION", "")
        
        if not xml_content or not view_name or not schema_name:
            logger.warning(f"Skipping calculation view without required data: {view_name}")
            continue
            
        # Extract source objects from XML
        source_objects = extract_source_objects(xml_content)
        
        if not source_objects:
            logger.warning(f"No source objects found for calculation view: {schema_name}.{view_name}")
            continue
            
        # Create qualified names for process and target
        process_qn = f"{connection_qn}/process/cv/{schema_name}.{view_name}"
        target_qn = f"{connection_qn}/DEFAULT/{schema_name}/{view_name}"
        
        # Create source entity references
        source_entities = []
        for src in source_objects:
            src_schema = src.get("schema", "")
            src_name = src.get("name", "")
            src_type = src.get("type", "Table")
            
            if src_schema and src_name:
                src_qn = f"{connection_qn}/DEFAULT/{src_schema}/{src_name}"
                source_entities.append({
                    "typeName": src_type,
                    "uniqueAttributes": {
                        "qualifiedName": src_qn
                    }
                })
        
        # Create process entity
        process_entity = {
            "PROCESS_NAME": f"Calculation View: {view_name}",
            "PROCESS_QUALIFIED_NAME": process_qn,
            "PROCESS_DESCRIPTION": f"Process that creates the calculation view {view_name} from source objects",
            "SOURCE_ENTITIES": json.dumps(source_entities),
            "TARGET_ENTITIES": json.dumps([{
                "typeName": "CalculationView",
                "uniqueAttributes": {
                    "qualifiedName": target_qn
                }
            }])
        }
        
        lineage_entities.append(process_entity)
    
    return lineage_entities 