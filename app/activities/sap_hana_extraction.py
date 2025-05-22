import os
from typing import Dict, Any, Optional, cast, List

import daft
from temporalio import activity

from application_sdk.activities.metadata_extraction.sql import (
    BaseSQLMetadataExtractionActivities,
    BaseSQLMetadataExtractionActivitiesState
)
from application_sdk.activities.common.models import ActivityStatistics
from application_sdk.common.utils import prepare_query
from application_sdk.common.logger_adaptors import get_logger
from application_sdk.activities.common.utils import auto_heartbeater
from application_sdk.inputs.parquet import ParquetInput
from application_sdk.outputs.parquet import ParquetOutput

from app.scripts.process_calculation_view import process_calculation_views

logger = get_logger(__name__)

class SAPHANAMetadataExtractionActivities(BaseSQLMetadataExtractionActivities):
    """Extended SQL extraction activities with SAP HANA calculation view support."""
    
    # SQL query for calculation views
    fetch_calculation_view_sql = """
    -- Extract calculation view information from SAP HANA
    SELECT 
        'DEFAULT' AS TABLE_CAT,
        SCHEMA_NAME AS TABLE_SCHEM,
        OBJECT_NAME AS VIEW_NAME,
        PACKAGE_ID AS PACKAGE_ID,
        VERSION_ID,
        ACTIVATED_AT,
        ACTIVATED_BY,
        CAST(CDATA AS NVARCHAR) AS ROUTINE_DEFINITION
    FROM
        _SYS_REPO.ACTIVE_OBJECT
        JOIN
        SYS.VIEWS ON concat(concat(PACKAGE_ID,'/'),OBJECT_NAME) = VIEW_NAME
    WHERE
        OBJECT_SUFFIX = 'calculationview'
    """
    
    # SQL query for calculation view columns
    fetch_calculation_view_column_sql = """
    -- Extract calculation view column information from SAP HANA
    SELECT 
        'DEFAULT' AS TABLE_CAT,
        sys_views.SCHEMA_NAME AS TABLE_SCHEM,
        active_object.PACKAGE_ID AS PACKAGE_ID,
        active_object.OBJECT_NAME AS VIEW_NAME,
        bimc_properties.COLUMN_NAME,
        bimc_properties.COLUMN_SQL_TYPE
    FROM
        _SYS_REPO.ACTIVE_OBJECT active_object 
        JOIN 
        _SYS_BI.BIMC_PROPERTIES bimc_properties ON active_object.OBJECT_NAME = bimc_properties.CUBE_NAME
        JOIN
        SYS.VIEWS sys_views ON concat(concat(active_object.PACKAGE_ID,'/'),active_object.OBJECT_NAME) = sys_views.VIEW_NAME  
    WHERE
        active_object.OBJECT_SUFFIX = 'calculationview' 
        AND bimc_properties.COLUMN_FLAG = 'Dimension Attribute'
    """
    
    @activity.defn
    @auto_heartbeater
    async def fetch_calculation_views(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[ActivityStatistics]:
        """Extract SAP HANA calculation view metadata from the source database.
        
        Args:
            workflow_args: Dictionary containing workflow configuration
                
        Returns:
            Optional[ActivityStatistics]: Statistics about the extracted entities, 
                or None if extraction failed
        """
        # Get the current state which contains the SQL client
        state = cast(
            BaseSQLMetadataExtractionActivitiesState,
            await self._get_state(workflow_args),
        )
        if not state.sql_client or not state.sql_client.engine:
            logger.error("SQL client or engine not initialized")
            raise ValueError("SQL client or engine not initialized")

        # Prepare the query with workflow arguments
        prepared_query = prepare_query(
            query=self.fetch_calculation_view_sql, 
            workflow_args=workflow_args
        )
        
        # Execute the query and store results
        statistics = await self.query_executor(
            sql_engine=state.sql_client.engine,
            sql_query=prepared_query,
            workflow_args=workflow_args,
            output_suffix="raw/calculation_view",
            typename="calculation_view",
        )
        return statistics
    
    @activity.defn
    @auto_heartbeater
    async def fetch_calculation_view_columns(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[ActivityStatistics]:
        """Extract SAP HANA calculation view column metadata from the source database.
        
        Args:
            workflow_args: Dictionary containing workflow configuration
                
        Returns:
            Optional[ActivityStatistics]: Statistics about the extracted entities, 
                or None if extraction failed
        """
        # Get the current state which contains the SQL client
        state = cast(
            BaseSQLMetadataExtractionActivitiesState,
            await self._get_state(workflow_args),
        )
        if not state.sql_client or not state.sql_client.engine:
            logger.error("SQL client or engine not initialized")
            raise ValueError("SQL client or engine not initialized")

        # Prepare the query with workflow arguments
        prepared_query = prepare_query(
            query=self.fetch_calculation_view_column_sql, 
            workflow_args=workflow_args
        )
        
        # Execute the query and store results
        statistics = await self.query_executor(
            sql_engine=state.sql_client.engine,
            sql_query=prepared_query,
            workflow_args=workflow_args,
            output_suffix="raw/calculation_view_column",
            typename="calculation_view_column",
        )
        return statistics
    
    @activity.defn
    @auto_heartbeater
    async def process_calculation_views(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[ActivityStatistics]:
        """Process calculation views to extract lineage and other metadata.
        
        Args:
            workflow_args: Dictionary containing workflow configuration
            
        Returns:
            Optional[ActivityStatistics]: Statistics about the processed data
        """
        try:
            # Get activity state
            state = cast(
                BaseSQLMetadataExtractionActivitiesState,
                await self._get_state(workflow_args),
            )
            
            # Validate and extract output parameters
            output_prefix, output_path, typename, workflow_id, workflow_run_id = (
                self._validate_output_args(workflow_args)
            )
            
            # Read previously extracted data
            calc_views_df = await self._load_data("calculation_view", workflow_args)
            tables_df = await self._load_data("table", workflow_args)
            views_df = await self._load_data("view", workflow_args)
            schemas_df = await self._load_data("schema", workflow_args)
            columns_df = await self._load_data("column", workflow_args)
            calc_view_columns_df = await self._load_data("calculation_view_column", workflow_args)
            
            # Convert DataFrames to lists of dictionaries
            calc_views = calc_views_df.to_pylist()
            tables = tables_df.to_pylist()
            views = views_df.to_pylist()
            schemas = schemas_df.to_pylist()
            columns = columns_df.to_pylist()
            calc_view_columns = calc_view_columns_df.to_pylist()
            
            # Process calculation views using the imported script
            processed_data = process_calculation_views(
                calc_views=calc_views,
                tables=tables,
                views=views,
                schemas=schemas,
                columns=columns,
                calc_view_columns=calc_view_columns
            )
            
            # Extract results
            processed_calc_views = processed_data.get("calc_views", [])
            lineage_processes = processed_data.get("lineage_processes", [])
            column_lineage = processed_data.get("column_lineage", [])
            
            # Write processed calculation views
            if processed_calc_views:
                processed_calc_views_df = daft.from_pylist(processed_calc_views)
                await self._write_output(
                    processed_calc_views_df, 
                    "processed/calculation_view", 
                    workflow_args
                )
            
            # Write lineage processes
            if lineage_processes:
                lineage_processes_df = daft.from_pylist(lineage_processes)
                await self._write_output(
                    lineage_processes_df, 
                    "processed/calc_view_lineage", 
                    workflow_args
                )
            
            # Write column lineage
            if column_lineage:
                column_lineage_df = daft.from_pylist(column_lineage)
                await self._write_output(
                    column_lineage_df, 
                    "processed/calc_view_column_lineage", 
                    workflow_args
                )
            
            # Return statistics about the processed data
            total_count = len(processed_calc_views) + len(lineage_processes) + len(column_lineage)
            return ActivityStatistics(
                count=total_count,
                type_name="calculation_view_processed"
            )
            
        except Exception as exc:
            logger.error(f"Error processing calculation views: {exc}")
            raise
    
    @activity.defn
    @auto_heartbeater
    async def process_calculation_view_lineage(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[ActivityStatistics]:
        """Process lineage from calculation views.
        
        This is a pass-through activity since the lineage is already generated in the
        process_calculation_views activity, but we keep it for workflow consistency.
        
        Args:
            workflow_args: Dictionary containing workflow configuration
            
        Returns:
            Optional[ActivityStatistics]: Statistics about the processed data
        """
        # This activity is a no-op since lineage is already processed
        # in the process_calculation_views activity
        try:
            output_prefix, output_path, typename, workflow_id, workflow_run_id = (
                self._validate_output_args(workflow_args)
            )
            
            # Check if the lineage data already exists
            lineage_input = ParquetInput(
                path=os.path.join(output_path, "processed/calc_view_lineage"),
                input_prefix=output_prefix,
                file_names=None,
                chunk_size=None,
            )
            
            try:
                lineage_df = await lineage_input.get_daft_dataframe()
                count = lineage_df.height()
            except:
                # No lineage data exists
                count = 0
                
            return ActivityStatistics(
                count=count,
                type_name="calc_view_lineage"
            )
            
        except Exception as exc:
            logger.error(f"Error processing calculation view lineage: {exc}")
            raise
    
    @activity.defn
    @auto_heartbeater
    async def process_calculation_view_column_lineage(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[ActivityStatistics]:
        """Process column-level lineage from calculation views.
        
        This is a pass-through activity since the column lineage is already generated in the
        process_calculation_views activity, but we keep it for workflow consistency.
        
        Args:
            workflow_args: Dictionary containing workflow configuration
            
        Returns:
            Optional[ActivityStatistics]: Statistics about the processed data
        """
        # This activity is a no-op since column lineage is already processed
        # in the process_calculation_views activity
        try:
            output_prefix, output_path, typename, workflow_id, workflow_run_id = (
                self._validate_output_args(workflow_args)
            )
            
            # Check if the column lineage data already exists
            column_lineage_input = ParquetInput(
                path=os.path.join(output_path, "processed/calc_view_column_lineage"),
                input_prefix=output_prefix,
                file_names=None,
                chunk_size=None,
            )
            
            try:
                column_lineage_df = await column_lineage_input.get_daft_dataframe()
                count = column_lineage_df.height()
            except:
                # No column lineage data exists
                count = 0
                
            return ActivityStatistics(
                count=count,
                type_name="calc_view_column_lineage"
            )
            
        except Exception as exc:
            logger.error(f"Error processing calculation view column lineage: {exc}")
            raise
    
    async def _load_data(self, data_type: str, workflow_args: Dict[str, Any]) -> daft.DataFrame:
        """Load data from a parquet file.
        
        Args:
            data_type: Type of data to load
            workflow_args: Workflow arguments
            
        Returns:
            daft.DataFrame: Loaded DataFrame
        """
        output_prefix, output_path, _, _, _ = self._validate_output_args(workflow_args)
        
        data_input = ParquetInput(
            path=os.path.join(output_path, f"raw/{data_type}"),
            input_prefix=output_prefix,
            file_names=None,
            chunk_size=None,
        )
        
        return await data_input.get_daft_dataframe()
    
    async def _write_output(
        self, df: daft.DataFrame, output_suffix: str, workflow_args: Dict[str, Any]
    ) -> None:
        """Write DataFrame to a parquet file.
        
        Args:
            df: DataFrame to write
            output_suffix: Output suffix
            workflow_args: Workflow arguments
        """
        output_prefix, output_path, _, _, _ = self._validate_output_args(workflow_args)
        
        output = ParquetOutput(
            output_prefix=output_prefix,
            output_path=output_path,
            output_suffix=output_suffix,
        )
        
        await output.write_daft_dataframe(df) 