"""
SAP HANA custom extraction activities.
"""
import os
import json
from typing import Dict, Any, Optional, List, cast

from temporalio import activity
import daft

from application_sdk.activities.metadata_extraction.sql import (
    BaseSQLMetadataExtractionActivities,
    BaseSQLMetadataExtractionActivitiesState
)
from application_sdk.activities.common.models import ActivityStatistics
from application_sdk.common.utils import prepare_query, read_sql_files
from application_sdk.common.logger_adaptors import get_logger
from application_sdk.activities.common.utils import auto_heartbeater
from application_sdk.inputs.parquet import ParquetInput
from application_sdk.outputs.parquet import ParquetOutput

# Import custom script for processing calculation view lineage
from app.scripts.process_calc_view_lineage import process_calc_view_lineage

logger = get_logger(__name__)

# Load SQL queries
queries = read_sql_files(queries_prefix="app/sql")

class SAPHANAExtractionActivities(BaseSQLMetadataExtractionActivities):
    """
    Custom extraction activities for SAP HANA database.
    
    This class extends the base SQL extraction activities to add
    SAP HANA specific functionality for calculation views.
    """
    
    # Define SQL queries for each extraction activity
    fetch_database_sql = queries.get("EXTRACT_DATABASE")
    fetch_schema_sql = queries.get("EXTRACT_SCHEMA")
    fetch_table_sql = queries.get("EXTRACT_TABLE")
    fetch_view_sql = queries.get("EXTRACT_VIEW")
    fetch_column_sql = queries.get("EXTRACT_COLUMN")
    fetch_procedure_sql = queries.get("EXTRACT_PROCEDURE")
    fetch_calc_view_sql = queries.get("EXTRACT_CALC_VIEW")
    fetch_calc_view_column_sql = queries.get("EXTRACT_CALC_VIEW_COLUMN")
    
    @activity.defn
    @auto_heartbeater
    async def fetch_calc_views(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[ActivityStatistics]:
        """
        Extract calculation view metadata from SAP HANA.
        
        Args:
            workflow_args: Dictionary containing workflow configuration
            
        Returns:
            ActivityStatistics: Statistics on extracted metadata
        """
        # Get the current state which contains the SQL client
        state = cast(
            BaseSQLMetadataExtractionActivitiesState,
            await self._get_state(workflow_args),
        )
        if not state.sql_client or not state.sql_client.engine:
            logger.error("SQL client or engine not initialized")
            raise ValueError("SQL client or engine not initialized")
        
        # Prepare query with workflow arguments
        prepared_query = prepare_query(
            query=self.fetch_calc_view_sql,
            workflow_args=workflow_args
        )
        
        # Execute query and store results
        statistics = await self.query_executor(
            sql_engine=state.sql_client.engine,
            sql_query=prepared_query,
            workflow_args=workflow_args,
            output_suffix="raw/calc_view",
            typename="calc_view",
        )
        return statistics
    
    @activity.defn
    @auto_heartbeater
    async def fetch_calc_view_columns(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[ActivityStatistics]:
        """
        Extract calculation view column metadata from SAP HANA.
        
        Args:
            workflow_args: Dictionary containing workflow configuration
            
        Returns:
            ActivityStatistics: Statistics on extracted metadata
        """
        # Get the current state which contains the SQL client
        state = cast(
            BaseSQLMetadataExtractionActivitiesState,
            await self._get_state(workflow_args),
        )
        if not state.sql_client or not state.sql_client.engine:
            logger.error("SQL client or engine not initialized")
            raise ValueError("SQL client or engine not initialized")
        
        # Prepare query with workflow arguments
        prepared_query = prepare_query(
            query=self.fetch_calc_view_column_sql,
            workflow_args=workflow_args
        )
        
        # Execute query and store results
        statistics = await self.query_executor(
            sql_engine=state.sql_client.engine,
            sql_query=prepared_query,
            workflow_args=workflow_args,
            output_suffix="raw/calc_view_column",
            typename="calc_view_column",
        )
        return statistics
    
    @activity.defn
    @auto_heartbeater
    async def process_calc_view_lineage(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[ActivityStatistics]:
        """
        Process calculation view definitions to extract lineage relationships.
        
        Args:
            workflow_args: Dictionary containing workflow configuration
            
        Returns:
            ActivityStatistics: Statistics on processed lineage data
        """
        try:
            # Extract output parameters
            output_prefix, output_path, _, _, _ = self._validate_output_args(workflow_args)
            
            # Load calculation view data
            calc_views_input = ParquetInput(
                path=os.path.join(output_path, "raw/calc_view"),
                input_prefix=output_prefix
            )
            calc_views_df = await calc_views_input.get_daft_dataframe()
            
            # Convert to list for processing
            calc_views_list = calc_views_df.to_pylist()
            
            # Get connection info
            connection_info = workflow_args.get("connection", {})
            
            # Process calculation view lineage using the imported function
            lineage_entities = process_calc_view_lineage(
                calc_views=calc_views_list,
                connection_info=connection_info
            )
            
            if not lineage_entities:
                logger.warning("No lineage entities were generated")
                return ActivityStatistics(count=0, type_name="calc_view_lineage")
            
            # Convert to DataFrame
            lineage_df = daft.from_pylist(lineage_entities)
            
            # Write lineage data to output
            lineage_output = ParquetOutput(
                output_prefix=output_prefix,
                output_path=output_path,
                output_suffix="raw/calc_view_lineage"
            )
            await lineage_output.write_daft_dataframe(lineage_df)
            
            return ActivityStatistics(
                count=len(lineage_entities),
                type_name="calc_view_lineage"
            )
            
        except Exception as e:
            logger.error(f"Error processing calculation view lineage: {str(e)}")
            raise 