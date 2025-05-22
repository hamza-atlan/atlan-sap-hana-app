"""
Main entry point for the SAP HANA connector application.
"""
import asyncio
import os

from application_sdk.application.metadata_extraction.sql import BaseSQLMetadataExtractionApplication
from application_sdk.constants import APPLICATION_NAME

from app.clients.sap_hana_client import SAPHANAClient
from app.handlers.sap_hana_handler import SAPHANAHandler
from app.transformers.sap_hana_transformer import SAPHANATransformer
from app.workflows.sap_hana_workflow import SAPHANAExtractionWorkflow
from application_sdk.common.logger_adaptors import get_logger

logger = get_logger(__name__)

async def main():
    """
    Initialize and start the SAP HANA connector application.
    """
    try:
        logger.info("Starting SAP HANA connector application")
        
        # Initialize the application with custom components
        application = BaseSQLMetadataExtractionApplication(
            name=APPLICATION_NAME or "sap-hana-connector",
            client_class=SAPHANAClient,
            handler_class=SAPHANAHandler,
            transformer_class=SAPHANATransformer,
        )
        
        # Setup workflow with custom workflow class and activities
        await application.setup_workflow(
            workflow_classes=[SAPHANAExtractionWorkflow]
        )
        
        # Start the worker to process workflow tasks
        await application.start_worker()
        
        # Setup the application server with custom workflow
        await application.setup_server(workflow_class=SAPHANAExtractionWorkflow)
        
        # Start the application server to accept API requests
        await application.start_server()
        
    except Exception as e:
        logger.error(f"Error starting SAP HANA connector application: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 