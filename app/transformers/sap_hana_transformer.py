"""
SAP HANA transformer for metadata transformation.
"""
import os
from typing import Any, Dict

from application_sdk.transformers.query import QueryBasedTransformer
from application_sdk.transformers.common.utils import get_yaml_query_template_path_mappings
from application_sdk.common.logger_adaptors import get_logger

logger = get_logger(__name__)


class SAPHANATransformer(QueryBasedTransformer):
    """
    SAP HANA transformer for metadata transformation.
    
    This transformer handles conversion of extracted SAP HANA metadata
    into standardized formats using YAML templates.
    """
    
    def __init__(self, connector_name: str, tenant_id: str, **kwargs: Any):
        """
        Initialize the SAP HANA transformer.
        
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

        # Define assets to be included
        template_assets = [
            # Standard asset types
            "TABLE", "COLUMN", "DATABASE", "SCHEMA", "VIEW", "PROCEDURE",
            # SAP HANA specific assets
            "CALCULATION-VIEW", "CALCULATION-VIEW-COLUMN",
            # Lineage assets
            "LINEAGE", "COLUMN-LINEAGE"
        ]
        
        # Update entity_class_definitions with templates
        updated_templates = get_yaml_query_template_path_mappings(
            custom_templates_path=custom_templates_path,
            assets=template_assets
        )
        self.entity_class_definitions.update(updated_templates)
        
        logger.info(f"Registered SAP HANA templates: {list(updated_templates.keys())}") 