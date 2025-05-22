from application_sdk.clients.sql import BaseSQLClient
from application_sdk.common.logger_adaptors import get_logger

logger = get_logger(__name__)

class SAPHANAClient(BaseSQLClient):
    """Custom SQL client for SAP HANA database connections."""
    
    # Define connection string template and requirements for SAP HANA
    DB_CONFIG = {
        "template": "hana://{username}:{password}@{host}:{port}",
        "required": ["username", "password", "host", "port"],
        "defaults": {
            "encrypt": "true",
            "sslValidateCertificate": "false"
        },
    }
    
    async def get_connection_string(self, credentials):
        """Generate connection string for SAP HANA.
        
        Args:
            credentials: Dictionary containing connection credentials
            
        Returns:
            str: Connection string for SAP HANA
        """
        auth_type = credentials.get("authType", "basic")
        
        # Basic username/password authentication
        if auth_type == "basic":
            # Verify required fields
            for field in self.DB_CONFIG["required"]:
                if field not in credentials:
                    raise ValueError(f"Missing required field for SAP HANA connection: {field}")
            
            # Build connection string with parameters
            connection_params = {**credentials}
            
            # Add SSL parameters if specified
            if credentials.get("encrypt", "true").lower() == "true":
                connection_params["encrypt"] = "true"
                connection_params["sslValidateCertificate"] = credentials.get(
                    "sslValidateCertificate", "false"
                )
            
            # Format the template with provided parameters
            template = self.DB_CONFIG["template"]
            connection_string = template.format(**connection_params)
            
            # Add query parameters
            query_params = []
            for param, value in self.DB_CONFIG["defaults"].items():
                if param not in connection_params:
                    query_params.append(f"{param}={value}")
            
            if query_params:
                connection_string += "?" + "&".join(query_params)
            
            logger.debug(f"Generated SAP HANA connection string (credentials masked)")
            return connection_string
        
        else:
            raise ValueError(f"Unsupported authentication type for SAP HANA: {auth_type}") 