"""
SAP HANA client for database connections.
"""
from typing import Dict, Any

from application_sdk.clients.sql import BaseSQLClient
from application_sdk.common.logger_adaptors import get_logger

logger = get_logger(__name__)

class SAPHANAClient(BaseSQLClient):
    """
    SAP HANA client for database connections.
    
    This client handles the connection to SAP HANA databases using the
    appropriate connection string format and authentication method.
    """
    
    # Define connection string template and requirements
    DB_CONFIG = {
        "template": "hana+hdbcli://{username}:{password}@{host}:{port}",
        "required": ["username", "password", "host", "port"],
        "defaults": {
            "encrypt": "true",
            "sslValidateCertificate": "false",
            "connect_timeout": 30,
        },
    }
    
    async def get_connection_string(self, credentials: Dict[str, Any]) -> str:
        """
        Generate a connection string for SAP HANA.
        
        Args:
            credentials: Dictionary containing connection credentials
            
        Returns:
            Connection string for SAP HANA
        """
        logger.info("Building SAP HANA connection string")
        
        # Validate required credentials
        for field in self.DB_CONFIG["required"]:
            if field not in credentials or not credentials[field]:
                raise ValueError(f"Missing required credential: {field}")
        
        # Build connection string from template
        connection_string = self.DB_CONFIG["template"].format(
            username=credentials["username"],
            password=credentials["password"],
            host=credentials["host"],
            port=credentials["port"]
        )
        
        # Add optional parameters if provided or use defaults
        query_params = []
        
        # Add defaults
        for key, value in self.DB_CONFIG.get("defaults", {}).items():
            if key not in credentials:
                query_params.append(f"{key}={value}")
        
        # Add any additional parameters from credentials
        for key, value in credentials.items():
            if key not in self.DB_CONFIG["required"] and key not in ["username", "password", "host", "port", "authType"]:
                query_params.append(f"{key}={value}")
        
        # Add query parameters to connection string
        if query_params:
            connection_string = f"{connection_string}?{'&'.join(query_params)}"
        
        return connection_string 