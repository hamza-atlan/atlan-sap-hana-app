"""
Process SAP HANA calculation views to extract metadata and lineage.
"""
from collections.abc import MutableMapping
from sqlitedict import SqliteDict
from typing import Any, Dict, List, Optional, Tuple

import daft

from application_sdk.common.logger_adaptors import get_logger
from app.models.sap_hana_models import MetadataHolder, ProcessorInput
from app.scripts.calc_view_lineage_extractor import CalcViewLineageExtractor
from app.utils.sap_hana_utils import (
    convert_to_list,
    convert_iterable_back_to_original_form,
    group_by_first_element,
    get_valid_table_keys,
    get_valid_calc_view_keys,
    get_valid_calc_view_column_keys,
    get_valid_table_view_column_keys,
    parse_xml,
    generate_process_key,
    generate_column_key,
    get_data_sources,
)

logger = get_logger(__name__)


class CalculationViewProcessor:
    """
    Process SAP HANA calculation views to extract metadata and lineage.
    
    This class processes the calculation views and their columns 
    to extract metadata and lineage information.
    
    Attributes:
        processor_input: Input data required for processing
        column_ordinal_map: Map to store column ordinals
        table_view_map: Map to store tables and views
        calc_view_map: Map to store calculation views
        calc_view_column_map: Map to store calculation view columns
        calc_tables_views_column_map: Map to store table/view columns
        calc_view_schema_map: Map to store calculation view schemas
        calc_processes_map: Map to store calculation view processes
    """
    
    def __init__(
            self,
            processor_input: ProcessorInput = ProcessorInput(),
            metadata_holder: MetadataHolder = MetadataHolder(),
    ):
        """
        Initialize the calculation view processor.
        
        Args:
            processor_input: Input data required for processing
            metadata_holder: Holder for metadata maps
        """
        self.processor_input: ProcessorInput = processor_input
        self.column_ordinal_map: MutableMapping[str, Dict[str, Any]] = metadata_holder.create_column_ordinal_map()
        self.table_view_map: MutableMapping[str, bool] = metadata_holder.create_table_view_map()
        self.calc_view_map: MutableMapping[str, bool] = metadata_holder.create_calc_view_map()
        self.calc_view_column_map: MutableMapping[str, bool] = metadata_holder.create_calc_view_column_map()
        self.calc_tables_views_column_map: MutableMapping[str, bool] = metadata_holder.create_tables_views_column_map()
        self.calc_view_schema_map: MutableMapping[str, str] = metadata_holder.create_calc_view_schema_map()
        self.calc_processes_map: MutableMapping[str, bool] = metadata_holder.create_processes_map()
        self.lineage_results: List[Dict[str, Any]] = []
        self.column_lineage_results: List[Dict[str, Any]] = []
        
    def collect_all_assets(self) -> None:
        """
        Collect all assets from the input data.
        
        This method collects tables, views, calculation views, and columns
        and stores them in the appropriate maps for later lookup.
        """
        for table_key in get_valid_table_keys(
                schema_name_key="TABLE_SCHEM",
                table_name_key="TABLE_NAME",
                table_iterable=self.processor_input.get_tables()
        ):
            self.table_view_map[table_key] = True

        for view_key in get_valid_table_keys(
                schema_name_key="TABLE_SCHEM",
                table_name_key="TABLE_NAME",
                table_iterable=self.processor_input.get_views()
        ):
            self.table_view_map[view_key] = True

        for table_view_column_key in get_valid_table_view_column_keys(
                schema_name_key="TABLE_SCHEM",
                table_name_key="TABLE_NAME",
                column_name_key="COLUMN_NAME",
                table_view_column_iterable=self.processor_input.get_tables_views_columns()
        ):
            self.calc_tables_views_column_map[table_view_column_key] = True

        for calc_view_key in get_valid_calc_view_keys(
                schema_name_key="TABLE_SCHEM",
                package_id_key="PACKAGE_ID",
                calc_view_name_key="VIEW_NAME",
                calc_view_iterable=self.processor_input.get_calc_views()
        ):
            self.calc_view_map[calc_view_key] = True

        for calc_view_column_key in get_valid_calc_view_column_keys(
                schema_name_key="TABLE_SCHEM",
                package_id_key="PACKAGE_ID",
                calc_view_name_key="VIEW_NAME",
                column_name_key="COLUMN_NAME",
                calc_view_column_iterable=self.processor_input.get_calc_view_columns()
        ):
            self.calc_view_column_map[calc_view_column_key] = True

        logger.info(f"Total tables/views: {len(self.table_view_map)}")
        logger.info(f"Total calculation views: {len(self.calc_view_map)}")
        logger.info(f"Total calculation view columns: {len(self.calc_view_column_map)}")
        
    def create_calc_view_schema_map(self) -> None:
        """
        Create a map of calculation view schemas.
        
        This map is used to look up the schema associated with a calculation view.
        """
        for calc_view in self.processor_input.get_calc_views():
            calc_view_name = calc_view.get('VIEW_NAME', None)
            calc_view_package_id = calc_view.get('PACKAGE_ID', None)
            calc_view_schema = calc_view.get('TABLE_SCHEM', None)

            if calc_view_package_id and calc_view_name and calc_view_schema:
                key = f"{calc_view_package_id}/{calc_view_name}"
                self.calc_view_schema_map[key] = calc_view_schema
        
    def collect_column_ordinals(self, calc_view: Optional[Dict[str, Any]]) -> None:
        """
        Collect column ordinals from a calculation view.
        
        Args:
            calc_view: Calculation view dictionary
        """
        if not calc_view or not calc_view.get("VIEW_NAME"):
            return
        view_name = calc_view.get("VIEW_NAME")
        column_ordinal_map = self.column_ordinal_map.get(view_name, {})
        parsed_view_data = calc_view.get("PARSED_DATA") if calc_view.get("PARSED_DATA") else {}
        calculation_scenario = parsed_view_data.get("Calculation:scenario")
        if not calculation_scenario:
            return
        logical_models = calculation_scenario.get("logicalModel")
        if not logical_models:
            return
        keys_to_process = {
            "attributes": "attribute",
            "baseMeasures": "measure",
            "calculatedMeasures": "measure",
            "calculatedAttributes": "calculatedAttribute",
        }

        for key, sub_key in keys_to_process.items():
            key_data = logical_models.get(key)
            if not key_data:
                continue
            columns = key_data.get(sub_key)
            if not columns:
                continue
            valid_columns = list(filter(
                lambda col: col.get("@id") and "$" not in col.get("@id"),
                convert_to_list(columns)
            ))
            if not valid_columns:
                continue
            for column in valid_columns:
                column_name = column.get("@id")
                column_ordinal = column.get("@order")
                column_description = (
                    column.get("descriptions", {}).get("@defaultDescription", "")
                    if column.get("descriptions", {})
                    else ""
                )
                column_object = {
                    "ordinal": column_ordinal,
                    "description": column_description,
                }
                column_ordinal_map[column_name] = column_object
            self.column_ordinal_map[view_name] = column_ordinal_map
            
    def process_calc_view(self, calc_view: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a calculation view to extract metadata.
        
        Args:
            calc_view: Calculation view dictionary
            
        Returns:
            Dict[str, Any]: Processed calculation view data
        """
        # Parse the XML data
        xml_data = calc_view.get("ROUTINE_DEFINITION", "")
        parsed_data = parse_xml(xml_data)
        calc_view["PARSED_DATA"] = parsed_data
        
        # Collect column ordinals
        self.collect_column_ordinals(calc_view)
        
        # Extract calculation view metadata
        calc_view_name = calc_view.get("VIEW_NAME", "")
        schema_name = calc_view.get("TABLE_SCHEM", "")
        package_id = calc_view.get("PACKAGE_ID", "")
        
        # Extract data sources
        data_sources = get_data_sources(calc_view)
        
        # Create metadata
        metadata = {
            "calc_view_name": calc_view_name,
            "schema_name": schema_name,
            "package_id": package_id,
            "data_sources": data_sources
        }
        
        return metadata

    def process_calc_view_lineage(self, calc_view: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a calculation view to extract lineage.
        
        Args:
            calc_view: Calculation view dictionary
            
        Returns:
            Dict[str, Any]: Lineage data
        """
        # Parse the XML data
        xml_data = calc_view.get("ROUTINE_DEFINITION", "")
        parsed_data = parse_xml(xml_data)
        
        # If no parsed data, return empty result
        if not parsed_data or "Calculation:scenario" not in parsed_data:
            return {"sources": []}
            
        # Extract calculation view metadata
        calc_view_name = calc_view.get("VIEW_NAME", "")
        schema_name = calc_view.get("TABLE_SCHEM", "")
        package_id = calc_view.get("PACKAGE_ID", "")
        
        # Extract lineage using the lineage extractor
        try:
            lineage_extractor = CalcViewLineageExtractor(parsed_data)
            column_lineage = lineage_extractor.extract_lineage()
            
            # Process source tables
            source_tables = set()
            for lineage in column_lineage:
                for source in lineage.get("source_columns", []):
                    source_type = source.get("source_type", "")
                    source_schema = source.get("source_table_schema", "")
                    source_table = source.get("source_table", "")
                    source_package_id = source.get("source_package_id", "")
                    
                    # Skip empty sources
                    if not source_schema or not source_table:
                        continue
                        
                    # Add to source tables set
                    if source_type == "CALCULATION_VIEW":
                        source_key = f"{source_schema}/{source_package_id}/{source_table}"
                    else:
                        source_key = f"{source_schema}/{source_table}"
                    source_tables.add((source_type, source_key))
            
            # Create lineage result
            lineage_result = {
                "target": {
                    "type": "CalculationView",
                    "name": calc_view_name,
                    "schema": schema_name,
                    "package_id": package_id,
                    "qualified_name": f"{schema_name}/{package_id}/{calc_view_name}"
                },
                "sources": [
                    {
                        "type": "Table" if src_type == "DATA_BASE_TABLE" else 
                               "View" if src_type == "DATA_BASE_VIEW" else
                               "CalculationView",
                        "qualified_name": src_key
                    }
                    for src_type, src_key in source_tables
                ],
                "column_lineage": column_lineage
            }
            
            # Store column lineage for later processing
            process_key = generate_process_key(schema_name, package_id, calc_view_name)
            self.calc_processes_map[process_key] = True
            
            return lineage_result
            
        except Exception as e:
            logger.error(f"Error extracting lineage for {calc_view_name}: {e}")
            return {"sources": []}

    def process_calc_view_column_lineage(self, calc_view: Dict[str, Any]) -> None:
        """
        Process column-level lineage for a calculation view.
        
        Args:
            calc_view: Calculation view dictionary
        """
        # Parse the XML data
        xml_data = calc_view.get("ROUTINE_DEFINITION", "")
        parsed_data = parse_xml(xml_data)
        
        # If no parsed data, return
        if not parsed_data or "Calculation:scenario" not in parsed_data:
            return
            
        # Extract calculation view metadata
        calc_view_name = calc_view.get("VIEW_NAME", "")
        schema_name = calc_view.get("TABLE_SCHEM", "")
        package_id = calc_view.get("PACKAGE_ID", "")
        
        # Generate process key
        process_key = generate_process_key(schema_name, package_id, calc_view_name)
        
        # Skip if process doesn't exist
        if process_key not in self.calc_processes_map:
            return
            
        # Extract lineage using the lineage extractor
        try:
            lineage_extractor = CalcViewLineageExtractor(parsed_data)
            column_lineage = lineage_extractor.extract_lineage()
            
            # Process column lineage
            for lineage in column_lineage:
                target_column = lineage.get("target_column", "")
                
                # Skip empty target columns
                if not target_column:
                    continue
                    
                # Generate target column key
                target_column_key = generate_column_key(
                    schema_name, package_id, calc_view_name, target_column
                )
                
                # Process source columns
                source_columns = []
                for source in lineage.get("source_columns", []):
                    source_type = source.get("source_type", "")
                    source_schema = source.get("source_table_schema", "")
                    source_table = source.get("source_table", "")
                    source_column = source.get("source_table_column", "")
                    source_package_id = source.get("source_package_id", "")
                    
                    # Skip empty sources
                    if not source_schema or not source_table or not source_column:
                        continue
                        
                    # Generate source column key
                    if source_type == "CALCULATION_VIEW":
                        source_column_key = generate_column_key(
                            source_schema, source_package_id, source_table, source_column
                        )
                    else:
                        source_column_key = str((source_schema, source_table, source_column))
                        
                    # Check if source column exists
                    if self.is_source_column_valid(source_type, source_column_key):
                        source_columns.append({
                            "type": "Column",
                            "qualified_name": source_column_key
                        })
                
                # Skip if no valid source columns
                if not source_columns:
                    continue
                    
                # Create column lineage result
                column_lineage_result = {
                    "process_id": process_key,
                    "target_column": {
                        "type": "Column",
                        "qualified_name": target_column_key,
                        "name": target_column
                    },
                    "source_columns": source_columns
                }
                
                # Add to column lineage results
                self.column_lineage_results.append(column_lineage_result)
                
        except Exception as e:
            logger.error(f"Error extracting column lineage for {calc_view_name}: {e}")

    def is_source_column_valid(self, source_type: str, source_column_key: str) -> bool:
        """
        Check if a source column is valid.
        
        Args:
            source_type: Source type (DATA_BASE_TABLE, DATA_BASE_VIEW, CALCULATION_VIEW)
            source_column_key: Source column key
            
        Returns:
            bool: True if the source column is valid, False otherwise
        """
        if source_type == "CALCULATION_VIEW":
            return source_column_key in self.calc_view_column_map
        else:
            return source_column_key in self.calc_tables_views_column_map
            
    def process_and_write_calc_views(self) -> List[Dict[str, Any]]:
        """
        Process all calculation views and extract metadata.
        
        Returns:
            List[Dict[str, Any]]: List of processed calculation views
        """
        logger.info("Processing calculation views...")
        calc_views_metadata = []
        
        for calc_view in self.processor_input.get_calc_views():
            try:
                metadata = self.process_calc_view(calc_view)
                calc_views_metadata.append(metadata)
            except Exception as e:
                logger.error(f"Error processing calculation view: {e}")
                
        logger.info(f"Processed {len(calc_views_metadata)} calculation views")
        return calc_views_metadata
        
    def process_and_write_calc_view_lineage(self) -> List[Dict[str, Any]]:
        """
        Process all calculation views and extract lineage.
        
        Returns:
            List[Dict[str, Any]]: List of lineage relationships
        """
        logger.info("Processing calculation view lineage...")
        lineage_results = []
        
        for calc_view in self.processor_input.get_calc_views():
            try:
                lineage = self.process_calc_view_lineage(calc_view)
                if lineage and lineage.get("sources"):
                    lineage_results.append(lineage)
            except Exception as e:
                logger.error(f"Error processing calculation view lineage: {e}")
                
        logger.info(f"Processed lineage for {len(lineage_results)} calculation views")
        self.lineage_results = lineage_results
        return lineage_results
        
    def process_and_write_calc_view_column_lineage(self) -> List[Dict[str, Any]]:
        """
        Process all calculation views and extract column-level lineage.
        
        Returns:
            List[Dict[str, Any]]: List of column lineage relationships
        """
        logger.info("Processing calculation view column lineage...")
        
        for calc_view in self.processor_input.get_calc_views():
            try:
                self.process_calc_view_column_lineage(calc_view)
            except Exception as e:
                logger.error(f"Error processing calculation view column lineage: {e}")
                
        logger.info(f"Processed column lineage: {len(self.column_lineage_results)} relationships")
        return self.column_lineage_results
        
    def run(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Run the calculation view processor.
        
        This method collects all assets, processes calculation views,
        extracts lineage, and returns the results.
        
        Returns:
            Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]: 
                Tuple containing:
                - List of processed calculation views
                - List of lineage relationships
                - List of column lineage relationships
        """
        logger.info("Starting calculation view processing...")
        self.collect_all_assets()
        self.create_calc_view_schema_map()
        
        # Process calculation views and extract metadata
        calc_views_metadata = self.process_and_write_calc_views()
        
        # Process calculation views and extract lineage
        lineage_results = self.process_and_write_calc_view_lineage()
        
        # Process calculation views and extract column lineage
        column_lineage_results = self.process_and_write_calc_view_column_lineage()
        
        logger.info("Calculation view processing completed.")
        return calc_views_metadata, lineage_results, column_lineage_results
        

# Functions for SDK activities to use
def process_calculation_views(calc_views_df: daft.DataFrame) -> daft.DataFrame:
    """
    Process calculation views to extract metadata.
    
    Args:
        calc_views_df: DataFrame of calculation views
        
    Returns:
        daft.DataFrame: DataFrame of processed calculation views
    """
    # Convert DataFrame to list of dictionaries
    calc_views = calc_views_df.to_pylist()
    
    # Create processor input
    processor_input = ProcessorInput(
        get_calc_views=lambda: calc_views,
    )
    
    # Create metadata holder with in-memory maps
    metadata_holder = MetadataHolder(
        create_column_ordinal_map=lambda: {},
        create_table_view_map=lambda: {},
        create_calc_view_map=lambda: {},
        create_calc_view_column_map=lambda: {},
        create_tables_views_column_map=lambda: {},
        create_calc_view_schema_map=lambda: {},
        create_processes_map=lambda: {},
    )
    
    # Create processor
    processor = CalculationViewProcessor(
        processor_input=processor_input,
        metadata_holder=metadata_holder,
    )
    
    # Process calculation views
    calc_views_metadata, _, _ = processor.run()
    
    # Return results as DataFrame
    return daft.from_pylist(calc_views_metadata)
    
def process_calculation_view_lineage(
    calc_views_df: daft.DataFrame, 
    calc_view_columns_df: Optional[daft.DataFrame] = None
) -> daft.DataFrame:
    """
    Process calculation views to extract lineage.
    
    Args:
        calc_views_df: DataFrame of calculation views
        calc_view_columns_df: DataFrame of calculation view columns
        
    Returns:
        daft.DataFrame: DataFrame of lineage relationships
    """
    # Convert DataFrames to lists of dictionaries
    calc_views = calc_views_df.to_pylist()
    calc_view_columns = calc_view_columns_df.to_pylist() if calc_view_columns_df is not None else []
    
    # Create processor input
    processor_input = ProcessorInput(
        get_calc_views=lambda: calc_views,
        get_calc_view_columns=lambda: calc_view_columns,
    )
    
    # Create metadata holder with in-memory maps
    metadata_holder = MetadataHolder(
        create_column_ordinal_map=lambda: {},
        create_table_view_map=lambda: {},
        create_calc_view_map=lambda: {},
        create_calc_view_column_map=lambda: {},
        create_tables_views_column_map=lambda: {},
        create_calc_view_schema_map=lambda: {},
        create_processes_map=lambda: {},
    )
    
    # Create processor
    processor = CalculationViewProcessor(
        processor_input=processor_input,
        metadata_holder=metadata_holder,
    )
    
    # Process calculation view lineage
    _, lineage_results, _ = processor.run()
    
    # Return results as DataFrame
    return daft.from_pylist(lineage_results)
    
def process_calculation_view_column_lineage(
    calc_views_df: daft.DataFrame,
    calc_view_columns_df: daft.DataFrame,
    columns_df: daft.DataFrame
) -> daft.DataFrame:
    """
    Process calculation views to extract column-level lineage.
    
    Args:
        calc_views_df: DataFrame of calculation views
        calc_view_columns_df: DataFrame of calculation view columns
        columns_df: DataFrame of table/view columns
        
    Returns:
        daft.DataFrame: DataFrame of column lineage relationships
    """
    # Convert DataFrames to lists of dictionaries
    calc_views = calc_views_df.to_pylist()
    calc_view_columns = calc_view_columns_df.to_pylist()
    columns = columns_df.to_pylist()
    
    # Create processor input
    processor_input = ProcessorInput(
        get_calc_views=lambda: calc_views,
        get_calc_view_columns=lambda: calc_view_columns,
        get_tables_views_columns=lambda: columns,
    )
    
    # Create metadata holder with persistent maps
    metadata_holder = MetadataHolder(
        create_column_ordinal_map=lambda: SqliteDict("column_ordinal_map.sqlite", autocommit=True),
        create_table_view_map=lambda: SqliteDict("table_view_map.sqlite", autocommit=True),
        create_calc_view_map=lambda: SqliteDict("calc_view_map.sqlite", autocommit=True),
        create_calc_view_column_map=lambda: SqliteDict("calc_view_column_map.sqlite", autocommit=True),
        create_tables_views_column_map=lambda: SqliteDict("tables_views_column_map.sqlite", autocommit=True),
        create_calc_view_schema_map=lambda: SqliteDict("calc_view_schema_map.sqlite", autocommit=True),
        create_processes_map=lambda: SqliteDict("processes_map.sqlite", autocommit=True),
    )
    
    # Create processor
    processor = CalculationViewProcessor(
        processor_input=processor_input,
        metadata_holder=metadata_holder,
    )
    
    # Process calculation view column lineage
    _, _, column_lineage_results = processor.run()
    
    # Return results as DataFrame
    return daft.from_pylist(column_lineage_results) 