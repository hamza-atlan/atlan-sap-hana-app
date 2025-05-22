from typing import Any, Callable, List, Sequence
from temporalio import workflow

from application_sdk.workflows.metadata_extraction.sql import BaseSQLMetadataExtractionWorkflow
from app.activities import SAPHANAMetadataExtractionActivities

@workflow.defn
class SAPHANAMetadataExtractionWorkflow(BaseSQLMetadataExtractionWorkflow):
    """Custom workflow that extends the base SQL extraction with SAP HANA specific activities.
    
    This workflow adds SAP HANA calculation view extraction and processing steps to the
    standard SQL metadata extraction sequence.
    """
    
    # Specify the activities class to use
    activities_cls = SAPHANAMetadataExtractionActivities
    
    @staticmethod
    def get_activities(
        activities: SAPHANAMetadataExtractionActivities,
    ) -> Sequence[Callable[..., Any]]:
        """Define the sequence of activities to execute.
        
        Args:
            activities: An instance of the activities class
            
        Returns:
            Sequence of activity methods to execute
        """
        # Start with the base activities
        standard_activities = [
            activities.preflight_check,
            activities.fetch_databases,
            activities.fetch_schemas,
            activities.fetch_tables,
            activities.fetch_views,
            activities.fetch_columns,
            activities.fetch_procedures,
        ]
        
        # Add SAP HANA specific activities
        sap_hana_activities = [
            activities.fetch_calculation_views,
            activities.fetch_calculation_view_columns,
            activities.process_calculation_views,
            activities.process_calculation_view_lineage,
            activities.process_calculation_view_column_lineage,
        ]
        
        # End with transform activity
        transform_activity = [
            activities.transform_data,
        ]
        
        # Return the complete sequence
        return standard_activities + sap_hana_activities + transform_activity
    
    def get_fetch_functions(self) -> List[Callable[..., Any]]:
        """Define the sequence of fetch functions to execute.
        
        This method determines which activities participate in the
        chunking and throttling mechanism of the workflow.
        
        Returns:
            List of fetch activity functions
        """
        # Get base fetch functions
        base_fetch_functions = super().get_fetch_functions()
        
        # Add SAP HANA specific fetch functions
        sap_hana_fetch_functions = [
            self.activities_cls.fetch_calculation_views,
            self.activities_cls.fetch_calculation_view_columns,
        ]
        
        # Return the combined list
        return base_fetch_functions + sap_hana_fetch_functions 