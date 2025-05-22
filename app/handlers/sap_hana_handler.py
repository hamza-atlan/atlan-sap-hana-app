"""
SAP HANA handler for metadata extraction.
"""
from typing import Any, Dict, Optional

from application_sdk.common.utils import read_sql_files
from application_sdk.handlers.sql import SQLHandler
from application_sdk.common.logger_adaptors import get_logger

logger = get_logger(__name__)

# Load all SQL queries
queries = read_sql_files(queries_prefix="app/sql")

class SAPHANAHandler(SQLHandler):
    """
    SAP HANA handler for metadata extraction.
    
    This handler implements preflight checks and other operations specific
    to SAP HANA database connections.
    """
    
    # SQL queries for various operations
    test_auth_sql = queries.get("TEST_AUTH")
    tables_check_sql = queries.get("TABLES_CHECK") 
    metadata_sql = queries.get("METADATA")
    
    async def preflight_check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run preflight checks for SAP HANA connections.
        
        This method validates database connection, version, and schema filters.
        
        Args:
            payload: Dictionary containing configuration parameters
            
        Returns:
            Dictionary with preflight check results
        """
        logger.info("Running SAP HANA preflight checks")
        
        # Run standard preflight checks from parent class
        results = await super().preflight_check(payload)
        
        # If base checks failed, return early
        if "error" in results:
            return results
            
        # Add SAP HANA specific checks if needed
        try:
            # Verify SAP HANA version compatibility
            client_version_result = await self.check_client_version(payload)
            results["clientVersionCheck"] = client_version_result
            
            if not client_version_result.get("success", False):
                results["error"] = f"SAP HANA version check failed: {client_version_result.get('failureMessage', '')}"
                return results
                
            # Return successful results
            return results
            
        except Exception as exc:
            error_msg = f"SAP HANA preflight check failed: {str(exc)}"
            logger.error(error_msg)
            results["error"] = error_msg
            return results
    
    async def check_client_version(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check SAP HANA database version.
        
        Args:
            payload: Dictionary containing configuration
            
        Returns:
            Dictionary with version check results
        """
        try:
            # Get SQL client from payload
            sql_client = payload.get("sql_client")
            if not sql_client or not sql_client.engine:
                raise ValueError("SQL client not initialized")
                
            # Execute version check query
            query = queries.get("CLIENT_VERSION")
            if not query:
                raise ValueError("Version check query not found")
                
            async with sql_client.engine.connect() as conn:
                result = await conn.execute(query)
                rows = await result.fetchall()
                
                if not rows:
                    raise ValueError("No version information returned")
                    
                version_str = str(rows[0][0]) if rows and rows[0] else ""
                
                # Parse version - assume format like "2.00.040.00.1553674765"
                major_version = int(version_str.split('.')[0]) if version_str and version_str.split('.') else 0
                
                # Check minimum version (SAP HANA 2.0 or higher)
                if major_version < 2:
                    return {
                        "success": False,
                        "failureMessage": f"SAP HANA version {version_str} is not supported. Minimum required version is 2.0.",
                        "successMessage": "",
                        "versionString": version_str
                    }
                
                return {
                    "success": True,
                    "successMessage": f"SAP HANA version {version_str} is supported.",
                    "failureMessage": "",
                    "versionString": version_str
                }
                
        except Exception as exc:
            logger.error(f"Error checking SAP HANA version: {exc}")
            return {
                "success": False,
                "failureMessage": f"Failed to check SAP HANA version: {str(exc)}",
                "successMessage": "",
                "versionString": ""
            } 