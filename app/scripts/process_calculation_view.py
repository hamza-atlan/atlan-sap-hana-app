"""Process SAP HANA calculation views to extract metadata and lineage."""

import json
import logging
from typing import Any, Dict, List, Optional, Iterable, MutableMapping
from collections.abc import MutableMapping as ABCMutableMapping

from application_sdk.common.logger_adaptors import get_logger
from app.scripts import utils
from app.scripts.parse_calc_view_lineage import process_calculation_view_lineage
from app.scripts.parse_calc_view_column_lineage import process_calculation_view_column_lineage

logger = get_logger(__name__)


class MetadataHolder:
    """Holds various metadata maps used during processing."""
    
    def create_column_ordinal_map(self) -> MutableMapping[str, Dict[str, Any]]:
        """Create an empty map for column ordinals."""
        try:
            from sqlitedict import SqliteDict
            return SqliteDict(":memory:", autocommit=True)
        except ImportError:
            logger.warning("SqliteDict not available, using in-memory dict instead")
            return {}
            
    def create_table_view_map(self) -> MutableMapping[str, bool]:
        """Create an empty map for tables and views."""
        try:
            from sqlitedict import SqliteDict
            return SqliteDict(":memory:", autocommit=True)
        except ImportError:
            logger.warning("SqliteDict not available, using in-memory dict instead")
            return {}
            
    def create_calc_view_map(self) -> MutableMapping[str, bool]:
        """Create an empty map for calculation views."""
        try:
            from sqlitedict import SqliteDict
            return SqliteDict(":memory:", autocommit=True)
        except ImportError:
            logger.warning("SqliteDict not available, using in-memory dict instead")
            return {}
            
    def create_calc_view_column_map(self) -> MutableMapping[str, bool]:
        """Create an empty map for calculation view columns."""
        try:
            from sqlitedict import SqliteDict
            return SqliteDict(":memory:", autocommit=True)
        except ImportError:
            logger.warning("SqliteDict not available, using in-memory dict instead")
            return {}
            
    def create_tables_views_column_map(self) -> MutableMapping[str, bool]:
        """Create an empty map for table/view columns."""
        try:
            from sqlitedict import SqliteDict
            return SqliteDict(":memory:", autocommit=True)
        except ImportError:
            logger.warning("SqliteDict not available, using in-memory dict instead")
            return {}
            
    def create_calc_view_schema_map(self) -> MutableMapping[str, str]:
        """Create an empty map for calculation view schemas."""
        try:
            from sqlitedict import SqliteDict
            return SqliteDict(":memory:", autocommit=True)
        except ImportError:
            logger.warning("SqliteDict not available, using in-memory dict instead")
            return {}
            
    def create_processes_map(self) -> MutableMapping[str, bool]:
        """Create an empty map for processes."""
        try:
            from sqlitedict import SqliteDict
            return SqliteDict(":memory:", autocommit=True)
        except ImportError:
            logger.warning("SqliteDict not available, using in-memory dict instead")
            return {}


class ProcessorInput:
    """Holds the input data for processing calculation views."""
    
    def __init__(
        self, 
        tables: Optional[List[Dict[str, Any]]] = None,
        views: Optional[List[Dict[str, Any]]] = None,
        schemas: Optional[List[Dict[str, Any]]] = None,
        tables_views_columns: Optional[List[Dict[str, Any]]] = None,
        calc_views: Optional[List[Dict[str, Any]]] = None,
        calc_view_columns: Optional[List[Dict[str, Any]]] = None
    ):
        """Initialize with input data.
        
        Args:
            tables: List of table objects
            views: List of view objects
            schemas: List of schema objects
            tables_views_columns: List of table/view column objects
            calc_views: List of calculation view objects
            calc_view_columns: List of calculation view column objects
        """
        self._tables = tables or []
        self._views = views or []
        self._schemas = schemas or []
        self._tables_views_columns = tables_views_columns or []
        self._calc_views = calc_views or []
        self._calc_view_columns = calc_view_columns or []
        
    def get_tables(self) -> List[Dict[str, Any]]:
        """Get tables list."""
        return self._tables
        
    def get_views(self) -> List[Dict[str, Any]]:
        """Get views list."""
        return self._views
        
    def get_schemas(self) -> List[Dict[str, Any]]:
        """Get schemas list."""
        return self._schemas
        
    def get_tables_views_columns(self) -> List[Dict[str, Any]]:
        """Get table/view columns list."""
        return self._tables_views_columns
        
    def get_calc_views(self) -> List[Dict[str, Any]]:
        """Get calculation views list."""
        return self._calc_views
        
    def get_calc_view_columns(self) -> List[Dict[str, Any]]:
        """Get calculation view columns list."""
        return self._calc_view_columns


class CalculationViewProcessor:
    """Process calculation view data to extract metadata and lineage."""
    
    def __init__(
        self,
        processor_input: ProcessorInput = None,
        metadata_holder: MetadataHolder = None
    ):
        """Initialize the processor.
        
        Args:
            processor_input: Input data for processing
            metadata_holder: Holder for metadata maps
        """
        self.processor_input = processor_input or ProcessorInput()
        metadata_holder = metadata_holder or MetadataHolder()
        self.column_ordinal_map = metadata_holder.create_column_ordinal_map()
        self.table_view_map = metadata_holder.create_table_view_map()
        self.calc_view_map = metadata_holder.create_calc_view_map()
        self.calc_view_column_map = metadata_holder.create_calc_view_column_map()
        self.calc_tables_views_column_map = metadata_holder.create_tables_views_column_map()
        self.calc_view_schema_map = metadata_holder.create_calc_view_schema_map()
        self.calc_processes_map = metadata_holder.create_processes_map()
        
    def run(self) -> Dict[str, List[Dict[str, Any]]]:
        """Run the calculation view processing workflow.
        
        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary with processed calculation views, lineage, and column lineage
        """
        logger.info("Processing calculation views...")
        self.collect_all_assets()
        self.create_calc_view_schema_map()
        
        # Process calculation views
        processed_calc_views = []
        for calc_view in self.processor_input.get_calc_views():
            processed_view = self.process_calc_view(calc_view)
            if processed_view:
                self.collect_column_ordinals(processed_view)
                processed_calc_views.append(processed_view)
        
        # Process lineage information
        lineage_processes = process_calculation_view_lineage(processed_calc_views)
        
        # Process column-level lineage information
        column_lineage = process_calculation_view_column_lineage(
            processed_calc_views, 
            self.processor_input.get_calc_view_columns()
        )
        
        logger.info("Processing calculation views completed.")
        
        return {
            "calc_views": processed_calc_views,
            "lineage_processes": lineage_processes,
            "column_lineage": column_lineage
        }
    
    def collect_all_assets(self) -> None:
        """Collect all assets from input data into maps for lookup."""
        for table_key in utils.get_valid_table_keys(
            schema_name_key="TABLE_SCHEM",
            table_name_key="TABLE_NAME",
            table_iterable=self.processor_input.get_tables()
        ):
            self.table_view_map[table_key] = True

        for view_key in utils.get_valid_table_keys(
            schema_name_key="TABLE_SCHEM",
            table_name_key="TABLE_NAME",
            table_iterable=self.processor_input.get_views()
        ):
            self.table_view_map[view_key] = True

        for table_view_column_key in utils.get_valid_table_view_column_keys(
            schema_name_key="TABLE_SCHEM",
            table_name_key="TABLE_NAME",
            column_name_key="COLUMN_NAME",
            table_view_column_iterable=self.processor_input.get_tables_views_columns()
        ):
            self.calc_tables_views_column_map[table_view_column_key] = True

        for calc_view_key in utils.get_valid_calc_view_keys(
            schema_name_key="TABLE_SCHEM",
            package_id_key="PACKAGE_ID",
            calc_view_name_key="VIEW_NAME",
            calc_view_iterable=self.processor_input.get_calc_views()
        ):
            self.calc_view_map[calc_view_key] = True

        for calc_view_column_key in utils.get_valid_calc_view_column_keys(
            schema_name_key="TABLE_SCHEM",
            package_id_key="PACKAGE_ID",
            calc_view_name_key="VIEW_NAME",
            column_name_key="COLUMN_NAME",
            calc_view_column_iterable=self.processor_input.get_calc_view_columns()
        ):
            self.calc_view_column_map[calc_view_column_key] = True

        logger.info(f"Total calculation views to process: {len(self.calc_view_map)}")
    
    def collect_column_ordinals(self, calc_view: Optional[Dict[str, Any]]) -> None:
        """Collect column ordinals from a calculation view.
        
        Args:
            calc_view: Calculation view to process
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
                utils.is_column_valid,
                utils.convert_to_list(columns)
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
    
    def create_calc_view_schema_map(self) -> None:
        """Create a map of calculation view schema names."""
        for calc_view in self.processor_input.get_calc_views():
            calc_view_name = calc_view.get('VIEW_NAME', None)
            calc_view_package_id = calc_view.get('PACKAGE_ID', None)
            schema_name = calc_view.get('TABLE_SCHEM', None)
            
            if calc_view_package_id and calc_view_name and schema_name:
                calc_view_key = f"{calc_view_package_id}/{calc_view_name}"
                self.calc_view_schema_map[calc_view_key] = schema_name
    
    def process_calc_view(self, calc_view: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a calculation view.
        
        Args:
            calc_view: Calculation view to process
            
        Returns:
            Optional[Dict[str, Any]]: Processed calculation view or None if processing failed
        """
        try:
            # Get key fields
            view_name = calc_view.get("VIEW_NAME", "")
            schema_name = calc_view.get("TABLE_SCHEM", "")
            package_id = calc_view.get("PACKAGE_ID", "")
            
            if not view_name or not schema_name or not package_id:
                logger.warning(f"Skipping calculation view with missing key fields: {calc_view}")
                return None
                
            # Parse the calculation view definition if it exists
            routine_def = calc_view.get("ROUTINE_DEFINITION")
            if routine_def:
                try:
                    if isinstance(routine_def, str):
                        calc_view["PARSED_DATA"] = json.loads(routine_def)
                except Exception as e:
                    logger.error(f"Error parsing calculation view definition: {e}")
                    
            # Count columns for this calculation view
            column_count = 0
            for col in self.processor_input.get_calc_view_columns():
                if (col.get("VIEW_NAME") == view_name and 
                    col.get("TABLE_SCHEM") == schema_name and
                    col.get("PACKAGE_ID") == package_id):
                    column_count += 1
                    
            calc_view["COLUMN_COUNT"] = column_count
            
            return calc_view
        except Exception as e:
            logger.error(f"Error processing calculation view: {e}")
            return None


def process_calculation_views(calc_views: List[Dict[str, Any]], 
                             tables: List[Dict[str, Any]],
                             views: List[Dict[str, Any]],
                             schemas: List[Dict[str, Any]],
                             columns: List[Dict[str, Any]],
                             calc_view_columns: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Process SAP HANA calculation views to extract metadata and lineage.
    
    Args:
        calc_views: List of calculation view objects
        tables: List of table objects
        views: List of view objects
        schemas: List of schema objects
        columns: List of table/view column objects
        calc_view_columns: List of calculation view column objects
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Dictionary with processed data
    """
    # Create processor input
    processor_input = ProcessorInput(
        tables=tables,
        views=views,
        schemas=schemas,
        tables_views_columns=columns,
        calc_views=calc_views,
        calc_view_columns=calc_view_columns
    )
    
    # Create processor and run
    processor = CalculationViewProcessor(processor_input=processor_input)
    return processor.run() 