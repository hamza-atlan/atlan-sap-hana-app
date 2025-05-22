import asyncio
from application_sdk.application.metadata_extraction.sql import BaseSQLMetadataExtractionApplication
from application_sdk.constants import APPLICATION_NAME

from app.clients import SAPHANAClient
from app.handlers import SAPHANAHandler
from app.activities import SAPHANAMetadataExtractionActivities
from app.transformers import SAPHANATransformer
from app.workflows import SAPHANAMetadataExtractionWorkflow

async def main():
    """Initialize and start the SAP HANA metadata extraction application."""
    # Initialize the application with custom components
    application = BaseSQLMetadataExtractionApplication(
        name=APPLICATION_NAME,
        client_class=SAPHANAClient,
        handler_class=SAPHANAHandler,
        transformer_class=SAPHANATransformer,
    )
    
    # Setup workflow with custom workflow class and activities
    await application.setup_workflow(
        workflow_classes=[SAPHANAMetadataExtractionWorkflow],
        activities_class=SAPHANAMetadataExtractionActivities
    )
    
    # Start the worker to process workflow tasks
    await application.start_worker()
    
    # Setup the application server with custom workflow
    await application.setup_server(workflow_class=SAPHANAMetadataExtractionWorkflow)
    
    # Start the application server to accept API requests
    await application.start_server()

if __name__ == "__main__":
    asyncio.run(main()) 