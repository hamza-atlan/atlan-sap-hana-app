"""Extract lineage information from SAP HANA calculation views."""

import json
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from application_sdk.common.logger_adaptors import get_logger
from app.scripts import utils

logger = get_logger(__name__)


class CalcViewLineageExtractor:
    """Extract lineage from calculation view data."""

    def __init__(self, json_data: Dict[str, Any]) -> None:
        """Initialize the lineage extractor with calculation view JSON data.

        Args:
            json_data: The parsed JSON data of the calculation view
        """
        self.__json_data: Dict[str, Any] = json_data
        self.__scenario: Dict[str, Any] = json_data["Calculation:scenario"]
        self.__scenario_name: str = self.__scenario.get("@id", "")
        self.__ds_map: Dict[str, Dict[str, str]] = (
            self.__build_data_sources(self.__scenario.get("dataSources", {}).get("DataSource", {}))
            if self.__scenario.get("dataSources", {})
            else {}
        )
        self.__calc_nodes_map: Dict[str, Dict[str, Any]] = (
            self.__build_calc_nodes(self.__scenario.get("calculationViews", {}).get("calculationView", {}))
            if self.__scenario.get("calculationViews", {})
            else {}
        )
        result = self.__gather_final_columns(self.__scenario, self.__calc_nodes_map)
        self.__final_node_id, self.__final_columns, self.__column_mappings = result[0], result[1], result[2]
        self.__explicit_columns = result[3] if len(result) > 3 else set()

    @staticmethod
    def __normalize_mappings(raw: Union[Dict[str, Any], List[Any]]) -> List[Dict[str, Any]]:
        """Normalize mappings to a list of dictionaries.

        Args:
            raw: Raw mapping data that might be a dictionary or a list

        Returns:
            List[Dict[str, Any]]: Normalized list of mapping dictionaries
        """
        if isinstance(raw, dict):
            return [raw]
        elif isinstance(raw, list):
            return [m for m in raw if isinstance(m, dict)]
        else:
            return []

    @staticmethod
    def __extract_inputs(node_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract input nodes from a calculation view node.

        Args:
            node_obj: The calculation view node object

        Returns:
            List[Dict[str, Any]]: List of input nodes
        """
        if "input" not in node_obj or node_obj["input"] is None:
            return []
        data: Union[Dict[str, Any], List[Dict[str, Any]]] = node_obj["input"]
        if isinstance(data, dict):
            return [data]
        elif isinstance(data, list):
            return data
        else:
            return []

    @staticmethod
    def __build_data_sources(data_sources_section: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Dict[str, str]]:
        """Build a map of data sources from the calculation view data.

        Args:
            data_sources_section: The data sources section from the calculation view data

        Returns:
            Dict[str, Dict[str, str]]: A map of data source IDs to their properties
        """
        ds_map: Dict[str, Dict[str, str]] = {}
        if isinstance(data_sources_section, dict):
            data_sources_section = [data_sources_section]

        for ds in data_sources_section:
            ds_id: str = "#" + ds["@id"]
            ds_type: str = ds.get("@type", "").upper()

            if ds_type in ["DATA_BASE_TABLE", "DATA_BASE_VIEW"]:
                schema: str = ds["columnObject"]["@schemaName"]
                table: str = ds["columnObject"]["@columnObjectName"]
                source_type: str = ds_type
                package_id: str = ""
            elif ds_type == "CALCULATION_VIEW":
                table: str = ds["@id"]
                schema: str = ds.get("columnObject", {}).get("@schemaName", "")
                source_type: str = ds_type
                package_id: str = utils.extract_package_from_resourceuri(ds.get("resourceUri", ""))
            else:
                schema = ""
                table = ""
                source_type = ""
                package_id = ""

            ds_map[ds_id] = {
                "schema": schema,
                "table": table,
                "source_type": source_type,
                "package_id": package_id,
            }

        return ds_map

    @staticmethod
    def __build_calc_nodes(calc_views_list: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
        """Build a map of calculation nodes from the calculation view data.

        Args:
            calc_views_list: List of calculation view nodes

        Returns:
            Dict[str, Dict[str, Any]]: A map of calculation node IDs to their data
        """
        if isinstance(calc_views_list, dict):
            calc_views_list = [calc_views_list]
        calc_map: Dict[str, Dict[str, Any]] = {}
        for node in calc_views_list:
            node_id: str = node["@id"]
            calc_map[node_id] = node
        return calc_map

    @staticmethod
    def __gather_final_columns(scenario: Dict[str, Any], calc_nodes_map: Dict[str, Dict[str, Any]]) -> Tuple[str, List[str], Dict[str, str], Set[str]]:
        """Gather the final columns from the calculation view.

        Args:
            scenario: The calculation scenario data
            calc_nodes_map: Map of calculation nodes

        Returns:
            Tuple[str, List[str], Dict[str, str], Set[str]]: A tuple containing the final node ID, the final columns,
                the column mappings, and the explicit columns
        """
        logical_model: Dict[str, Any] = scenario["logicalModel"]
        final_node_id: str = logical_model["@id"]
        final_columns: Set[str] = set()
        # Dictionary to map logical column names to calculation view column names
        column_mappings: Dict[str, str] = {}
        # Set to track explicitly defined columns in the logical model
        explicit_columns: Set[str] = set()

        # Process attributes from the logical model
        if "attributes" in logical_model and logical_model["attributes"]:
            attrs: Union[Dict[str, Any], List[Dict[str, Any]]] = logical_model["attributes"]["attribute"]
            if isinstance(attrs, dict):
                attrs = [attrs]
            for a in attrs:
                attr_id = a["@id"]
                final_columns.add(attr_id)
                explicit_columns.add(attr_id)  # Mark as explicitly defined
                # Get the mapping between logical model attribute and calculation view column
                if "keyMapping" in a and "@columnName" in a["keyMapping"]:
                    column_mappings[attr_id] = a["keyMapping"]["@columnName"]

        if "baseMeasures" in logical_model and logical_model["baseMeasures"]:
            measures: Union[Dict[str, Any], List[Dict[str, Any]]] = logical_model["baseMeasures"]["measure"]
            if isinstance(measures, dict):
                measures = [measures]
            for m in measures:
                measure_id = m["@id"]
                final_columns.add(measure_id)
                explicit_columns.add(measure_id)  # Mark as explicitly defined
                # Get the mapping between logical model measure and calculation view column
                if "measureMapping" in m and "@columnName" in m["measureMapping"]:
                    column_mappings[measure_id] = m["measureMapping"]["@columnName"]

        if final_node_id in calc_nodes_map:
            node_obj: Dict[str, Any] = calc_nodes_map[final_node_id]
            if "viewAttributes" in node_obj and node_obj["viewAttributes"]:
                va: Union[Dict[str, Any], List[Dict[str, Any]]] = node_obj["viewAttributes"]["viewAttribute"]
                if isinstance(va, dict):
                    va = [va]
                for v in va:
                    col_id: Optional[str] = v.get("@id")
                    if v.get("@hidden", "false") == "true":
                        continue
                    if col_id:
                        final_columns.add(col_id)
                        # For direct view attributes, they usually have the same name in calculation view
                        if col_id not in column_mappings:
                            column_mappings[col_id] = col_id

        return final_node_id, sorted(final_columns), column_mappings, explicit_columns

    def __get_lineage(
        self,
        node_id: str,
        column_name: str,
        visited: Optional[Set[Tuple[str, str]]] = None,
    ) -> List[Dict[str, str]]:
        """Get lineage information for a specific column in a node.

        Args:
            node_id: ID of the calculation node
            column_name: Name of the column
            visited: Set of already visited node and column combinations to prevent infinite recursion

        Returns:
            List[Dict[str, str]]: List of source information dictionaries
        """
        if visited is None:
            visited = set()
        visited_key: Tuple[str, str] = (node_id, column_name)
        if visited_key in visited:
            return []
        visited.add(visited_key)

        # Check if the node is a calculation view.
        if node_id not in self.__calc_nodes_map:
            # If not, maybe it is a data source.
            ds_key = "#" + node_id  # because ds_map keys are stored with a leading '#'
            if ds_key in self.__ds_map:
                ds_info: Dict[str, str] = self.__ds_map[ds_key]
                return [{
                    "source_type": ds_info["source_type"],
                    "source_table_schema": ds_info["schema"],
                    "source_table": ds_info["table"],
                    "source_table_column": column_name,
                    "source_package_id": ds_info["package_id"],
                }]
            return []

        node_obj: Dict[str, Any] = self.__calc_nodes_map[node_id]
        results: List[Dict[str, str]] = []
        inputs: List[Dict[str, Any]] = self.__extract_inputs(node_obj)

        for inp in inputs:
            upstream_node_id: str = inp.get("@node", "")
            raw_mappings: Union[Dict[str, Any], List[Any]] = inp.get("mapping", [])
            mappings: List[Dict[str, Any]] = self.__normalize_mappings(raw_mappings)
            for m in mappings:
                if m.get("@target") == column_name:
                    if "ConstantAttributeMapping" in m.get("@xsi:type", ""):
                        continue
                    src_col: str = m["@source"]
                    if upstream_node_id in self.__ds_map:
                        ds_info: Dict[str, str] = self.__ds_map[upstream_node_id]
                        results.append(
                            {
                                "source_type": ds_info["source_type"],
                                "source_table_schema": ds_info["schema"],
                                "source_table": ds_info["table"],
                                "source_table_column": src_col,
                                "source_package_id": ds_info["package_id"],
                            }
                        )
                    else:
                        up_id: str = upstream_node_id.lstrip("#")
                        sub_lineage: List[Dict[str, str]] = self.__get_lineage(
                            up_id, src_col, visited
                        )
                        results.extend(sub_lineage)

        return results

    def extract_lineage(self) -> List[Dict[str, str]]:
        """Extract lineage information for all columns in the calculation view.

        Returns:
            List[Dict[str, str]]: List of column lineage information dictionaries
        """
        all_lineage: List[Dict[str, str]] = []

        # For each column in the final result, trace back its sources
        for col in self.__final_columns:
            # Get the internal column name using the mapping
            internal_col = self.__column_mappings.get(col, col)
            sources = self.__get_lineage(self.__final_node_id, internal_col)
            
            for source in sources:
                lineage_item = {
                    "calc_view_schema": source.get("calc_view_schema", ""),
                    "calc_view_name": self.__scenario_name,
                    "calc_view_column": col,
                    "calc_view_package_id": source.get("calc_view_package_id", ""),
                    "source_type": source.get("source_type", ""),
                    "source_schema": source.get("source_table_schema", ""),
                    "source_table": source.get("source_table", ""),
                    "source_table_column": source.get("source_table_column", ""),
                    "source_package_id": source.get("source_package_id", "")
                }
                all_lineage.append(lineage_item)
                
        return all_lineage


def extract_lineage_from_calculation_view(calc_view: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract lineage from a calculation view.

    Args:
        calc_view: Dictionary containing calculation view data with ROUTINE_DEFINITION
            containing the calculation view definition as XML/JSON

    Returns:
        List[Dict[str, str]]: List of column lineage information dictionaries
    """
    routine_def = calc_view.get("ROUTINE_DEFINITION", "")
    if not routine_def:
        return []
        
    try:
        # Parse the calculation view definition
        parsed_data = json.loads(routine_def)
        
        # Add schema information to the parsed data
        schema_name = calc_view.get("TABLE_SCHEM", "")
        
        # Extract lineage information
        extractor = CalcViewLineageExtractor(parsed_data)
        lineage_items = extractor.extract_lineage()
        
        # Add calculation view schema information to each lineage item
        for item in lineage_items:
            item["calc_view_schema"] = schema_name
            item["calc_view_package_id"] = calc_view.get("PACKAGE_ID", "")
            
        return lineage_items
    except Exception as e:
        logger.error(f"Error extracting lineage from calculation view: {e}")
        return []


def process_calculation_view_lineage(calc_views: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process calculation view data to extract lineage information.
    
    Args:
        calc_views: List of calculation view dictionaries with parsed data
        
    Returns:
        List[Dict[str, Any]]: List of lineage process entities
    """
    lineage_processes = []
    
    for calc_view in calc_views:
        # Skip if there's no routine definition
        if not calc_view.get("ROUTINE_DEFINITION"):
            continue
            
        # Get information about the calculation view
        calc_view_name = calc_view.get("VIEW_NAME", "")
        schema_name = calc_view.get("TABLE_SCHEM", "")
        package_id = calc_view.get("PACKAGE_ID", "")
        
        try:
            # Parse the calculation view definition if it's a string
            parsed_data = None
            routine_def = calc_view.get("ROUTINE_DEFINITION")
            if isinstance(routine_def, str):
                parsed_data = json.loads(routine_def)
            else:
                parsed_data = routine_def
                
            # Add parsed data to the calculation view
            calc_view["PARSED_DATA"] = parsed_data
            
            # Extract source tables and views used in this calculation view
            sources = []
            if parsed_data and "Calculation:scenario" in parsed_data:
                scenario = parsed_data["Calculation:scenario"]
                if "dataSources" in scenario and "DataSource" in scenario["dataSources"]:
                    data_sources = utils.convert_to_list(scenario["dataSources"]["DataSource"])
                    for ds in data_sources:
                        ds_type = ds.get("@type", "").upper()
                        if ds_type in ["DATA_BASE_TABLE", "DATA_BASE_VIEW", "CALCULATION_VIEW"]:
                            source_schema = ds.get("columnObject", {}).get("@schemaName", "")
                            source_name = ""
                            
                            if ds_type == "CALCULATION_VIEW":
                                source_name = ds.get("@id", "")
                                source_package_id = utils.extract_package_from_resourceuri(ds.get("resourceUri", ""))
                                source_full_name = f"{source_schema}.{source_package_id}.{source_name}"
                            else:
                                source_name = ds.get("columnObject", {}).get("@columnObjectName", "")
                                source_package_id = ""
                                source_full_name = f"{source_schema}.{source_name}"
                                
                            sources.append({
                                "SOURCE_NAME": source_full_name,
                                "SOURCE_TABLE_TYPE": ds_type
                            })
            
            # Create a lineage process entity if there are sources
            if sources:
                # Unique target calculation view identifier: schema.package_id.view_name
                target_calc_view = f"{schema_name}.{package_id}.{calc_view_name}"
                
                # Format inputs for the transformer
                inputs_json = []
                for source in sources:
                    source_type = source["SOURCE_TABLE_TYPE"]
                    if source_type == "DATA_BASE_TABLE":
                        entity_type = "Table"
                    elif source_type == "DATA_BASE_VIEW":
                        entity_type = "View"
                    else:
                        entity_type = "CalculationView"
                        
                    inputs_json.append({
                        "typeName": entity_type,
                        "uniqueAttributes": {
                            "qualifiedName": source["SOURCE_NAME"]
                        }
                    })
                
                # Create lineage process entity
                lineage_process = {
                    "TARGET_CALC_VIEW": target_calc_view,
                    "TARGET_CALC_VIEW_NAME": calc_view_name,
                    "SOURCES": sources,
                    "SOURCE_JOIN": ", ".join([s["SOURCE_NAME"] for s in sources]),
                    "INPUTS": json.dumps(inputs_json)
                }
                
                lineage_processes.append(lineage_process)
        
        except Exception as e:
            logger.error(f"Error processing lineage for calculation view {calc_view_name}: {e}")
    
    return lineage_processes 