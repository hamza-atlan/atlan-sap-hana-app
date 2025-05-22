"""
Calculation view lineage extractor for SAP HANA.
"""
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from application_sdk.common.logger_adaptors import get_logger
from app.utils.sap_hana_utils import convert_to_list

logger = get_logger(__name__)


class CalcViewLineageExtractor:
    """
    Extracts lineage information from SAP HANA calculation views.
    
    This class parses the JSON representation of a calculation view
    and extracts the lineage relationships between columns.
    """
    
    def __init__(self, json_data: Dict[str, Any]) -> None:
        """
        Initialize the lineage extractor.
        
        Args:
            json_data: JSON representation of a calculation view
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
        """
        Normalize mapping data to a list of dictionaries.
        
        Args:
            raw: Raw mapping data
            
        Returns:
            List[Dict[str, Any]]: Normalized mapping data
        """
        if isinstance(raw, dict):
            return [raw]
        elif isinstance(raw, list):
            return [m for m in raw if isinstance(m, dict)]
        else:
            return []

    @staticmethod
    def __extract_inputs(node_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract input nodes from a calculation node.
        
        Args:
            node_obj: Calculation node object
            
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
        """
        Build a map of data sources.
        
        Args:
            data_sources_section: Data sources section of the calculation view
            
        Returns:
            Dict[str, Dict[str, str]]: Map of data sources
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
                schema: str = ds["columnObject"]["@schemaName"]
                source_type: str = ds_type
                package_id: str = ""
                if "resourceUri" in ds:
                    from app.utils.sap_hana_utils import extract_package_from_resourceuri
                    package_id = extract_package_from_resourceuri(ds["resourceUri"])
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
        """
        Build a map of calculation nodes.
        
        Args:
            calc_views_list: List of calculation view nodes
            
        Returns:
            Dict[str, Dict[str, Any]]: Map of calculation nodes
        """
        if isinstance(calc_views_list, dict):
            calc_views_list = [calc_views_list]
        calc_map: Dict[str, Dict[str, Any]] = {}
        for node in calc_views_list:
            node_id: str = node["@id"]
            calc_map[node_id] = node
        return calc_map

    @staticmethod
    def __gather_final_columns(
        scenario: Dict[str, Any], 
        calc_nodes_map: Dict[str, Dict[str, Any]]
    ) -> Tuple[str, List[str], Dict[str, str], Set[str]]:
        """
        Gather final columns from the logical model.
        
        Args:
            scenario: Calculation scenario
            calc_nodes_map: Map of calculation nodes
            
        Returns:
            Tuple[str, List[str], Dict[str, str], Set[str]]: Tuple containing:
                - Final node ID
                - List of final columns
                - Map of column mappings
                - Set of explicitly defined columns
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
        """
        Get lineage for a column in a node.
        
        Args:
            node_id: Node ID
            column_name: Column name
            visited: Set of visited node-column pairs
            
        Returns:
            List[Dict[str, str]]: List of source columns
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
        inputs: List[Dict[str, Any]] = self.__extract_inputs(node_obj)
        results: List[Dict[str, str]] = []

        for inp in inputs:
            upstream_node_id: str = inp.get("@node")
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

    def extract_lineage(self) -> List[Dict[str, Any]]:
        """
        Extract lineage for all columns in the calculation view.
        
        Returns:
            List[Dict[str, Any]]: List of column lineage mappings
        """
        result: List[Dict[str, Any]] = []
        
        for column_name in self.__final_columns:
            if column_name not in self.__column_mappings:
                logger.warning(f"Column {column_name} not found in mapping")
                continue
                
            calculation_view_column = self.__column_mappings[column_name]
            source_columns = self.__get_lineage(self.__final_node_id, calculation_view_column)
            
            lineage_mapping = {
                "target_column": column_name,
                "calculation_view_column": calculation_view_column,
                "source_columns": source_columns,
                "is_explicit": column_name in self.__explicit_columns
            }
            
            result.append(lineage_mapping)
            
        return result 