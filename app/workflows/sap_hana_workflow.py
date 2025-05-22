"""
SAP HANA metadata extraction workflow.
"""
from typing import Any, Callable, List, Sequence

from temporalio import workflow

from application_sdk.workflows.metadata_extraction.sql import BaseSQLMetadataExtractionWorkflow
from app.activities.sap_hana_activities import SAPHANAExtractionActivities

@workflow.defn
class SAPHANAExtractionWorkflow(BaseSQLMetadataExtractionWorkflow):
    """
    SAP HANA metadata extraction workflow.
    
    This workflow extends the base SQL metadata extraction workflow to add
    SAP HANA specific activities for calculation views.
    """
    
    # Specify the activities class to use
    activities_cls = SAPHANAExtractionActivities
    
    @staticmethod
    def get_activities(
        activities: SAPHANAExtractionActivities,
    ) -> Sequence[Callable[..., Any]]:
        """
        Define the sequence of activities to execute.
        
        Args:
            activities: An instance of the SAP HANA activities class
            
        Returns:
            Sequence of activity methods to execute
        """
        # Start with standard extraction activities
        standard_activities = [
            activities.preflight_check,
            activities.fetch_databases,
            activities.fetch_schemas,
            activities.fetch_tables,
            activities.fetch_views,
            activities.fetch_columns,
            activities.fetch_procedures
        ]
        
        # Add SAP HANA specific activities
        sap_hana_activities = [
            activities.fetch_calc_views,
            activities.fetch_calc_view_columns,
            activities.process_calc_view_lineage
        ]
        
        # End with transform activity
        transform_activity = [
            activities.transform_data,
        ]
        
        # Return the complete sequence
        return standard_activities + sap_hana_activities + transform_activity
    
    def get_fetch_functions(self) -> List[Callable]:
        """
        Define the fetch functions to use for chunking.
        
        Returns:
            List of fetch activity functions
        """
        # Get base fetch functions
        base_fetch_functions = super().get_fetch_functions()
        
        # Add SAP HANA specific fetch functions
        sap_hana_fetch_functions = [
            self.activities_cls.fetch_calc_views,
            self.activities_cls.fetch_calc_view_columns
        ]
        
        # Return combined list
        return base_fetch_functions + sap_hana_fetch_functions 