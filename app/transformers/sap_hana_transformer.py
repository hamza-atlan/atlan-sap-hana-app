import os
from typing import Any, Dict, Optional

import daft
from application_sdk.transformers.query import QueryBasedTransformer
from application_sdk.transformers.common.utils import get_yaml_query_template_path_mappings
from application_sdk.common.logger_adaptors import get_logger

logger = get_logger(__name__)

class SAPHANATransformer(QueryBasedTransformer):
    """Custom transformer that adds support for SAP HANA specific entity types."""
    
    def __init__(self, connector_name: str, tenant_id: str, **kwargs: Any):
        """Initialize the transformer with SAP HANA specific templates.
        
        Args:
            connector_name: Name of the connector
            tenant_id: Tenant ID
            **kwargs: Additional arguments
        """
        # Initialize base class first - this sets up standard entity mappings
        super().__init__(connector_name, tenant_id, **kwargs)
        
        # Get path to custom templates directory
        custom_templates_path = os.path.join(
            os.path.dirname(__file__), "templates"
        )

        # Define SAP HANA specific assets to be included alongside standard ones
        hana_assets = [
            "CALCULATION-VIEW",
            "CALCULATION-VIEW-COLUMN",
            "CALC-VIEW-LINEAGE",
            "CALC-VIEW-COLUMN-LINEAGE"
        ]
        
        # Update entity_class_definitions with both standard and custom templates
        updated_templates = get_yaml_query_template_path_mappings(
            custom_templates_path=custom_templates_path,
            assets=[
                # Standard asset types
                "TABLE", "COLUMN", "DATABASE", "SCHEMA", 
                "VIEW", "PROCEDURE",
                # SAP HANA specific asset types
                *hana_assets
            ]
        )
        self.entity_class_definitions.update(updated_templates)
        
        logger.info(f"Registered SAP HANA templates: {list(updated_templates.keys())}")
        
    async def transform_with_preprocessing(
        self, 
        typename: str, 
        df: daft.DataFrame, 
        preprocessing_func: Optional[callable] = None, 
        **kwargs: Any
    ) -> daft.DataFrame:
        """Transform data with optional preprocessing.
        
        Args:
            typename: The type name for transformation
            df: Input DataFrame
            preprocessing_func: Optional function to preprocess data
            **kwargs: Additional arguments for preprocessing
            
        Returns:
            daft.DataFrame: Transformed DataFrame
        """
        if preprocessing_func:
            df = preprocessing_func(df, **kwargs)
            
        return await self.transform_metadata(typename, df) 