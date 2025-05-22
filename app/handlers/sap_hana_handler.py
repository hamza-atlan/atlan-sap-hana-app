from typing import Dict, Any
from application_sdk.handlers.sql import SQLHandler
from application_sdk.common.logger_adaptors import get_logger

logger = get_logger(__name__)

class SAPHANAHandler(SQLHandler):
    """Custom handler for SAP HANA with specialized preflight checks."""
    
    async def preflight_check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Extend preflight check with SAP HANA-specific validation.
        
        Args:
            payload: Dictionary containing workflow payload with credentials
            
        Returns:
            Dict[str, Any]: Preflight check results dictionary
        """
        # First run standard preflight checks
        base_results = await super().preflight_check(payload)
        
        # If base checks failed, return early
        if "error" in base_results:
            return base_results
            
        # Run SAP HANA specific validation
        try:
            hana_check_result = await self.validate_hana_requirements(payload)
            base_results["hanaCheck"] = hana_check_result
            
            # Update overall success based on custom check
            if not hana_check_result["success"]:
                raise ValueError(f"SAP HANA check failed: {hana_check_result['failureMessage']}")
                
            return base_results
            
        except Exception as exc:
            base_results["error"] = f"SAP HANA preflight check failed: {str(exc)}"
            return base_results
    
    async def validate_hana_requirements(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Validate SAP HANA specific requirements.
        
        Args:
            payload: Dictionary containing workflow payload with credentials
            
        Returns:
            Dict[str, Any]: Validation result dictionary
        """
        try:
            # Get the SQL client to execute validation query
            client = await self.get_client(payload)
            
            # Check for calculation view access privileges
            calc_view_access_query = """
            SELECT COUNT(*) as access_count 
            FROM _SYS_REPO.ACTIVE_OBJECT 
            WHERE OBJECT_SUFFIX = 'calculationview' 
            LIMIT 1
            """
            
            try:
                calc_view_access_result = await client.execute_query(calc_view_access_query)
                have_calc_view_access = calc_view_access_result and calc_view_access_result.get("access_count", 0) >= 0
            except Exception as e:
                logger.warning(f"Failed to check calculation view access: {e}")
                have_calc_view_access = False
            
            # Check for BIMC_PROPERTIES access (needed for column metadata)
            bimc_access_query = """
            SELECT COUNT(*) as access_count 
            FROM _SYS_BI.BIMC_PROPERTIES 
            WHERE COLUMN_FLAG = 'Dimension Attribute' 
            LIMIT 1
            """
            
            try:
                bimc_access_result = await client.execute_query(bimc_access_query)
                have_bimc_access = bimc_access_result and bimc_access_result.get("access_count", 0) >= 0
            except Exception as e:
                logger.warning(f"Failed to check BIMC_PROPERTIES access: {e}")
                have_bimc_access = False
            
            # Return validation result
            if have_calc_view_access and have_bimc_access:
                return {
                    "success": True,
                    "successMessage": "SAP HANA calculation view access verified",
                    "failureMessage": ""
                }
            elif not have_calc_view_access:
                return {
                    "success": False,
                    "successMessage": "",
                    "failureMessage": "Missing access to _SYS_REPO.ACTIVE_OBJECT - calculation views cannot be extracted",
                    "error": "Insufficient privileges for calculation view extraction"
                }
            else:
                return {
                    "success": False,
                    "successMessage": "",
                    "failureMessage": "Missing access to _SYS_BI.BIMC_PROPERTIES - column metadata cannot be extracted",
                    "error": "Insufficient privileges for calculation view column extraction"
                }
                
        except Exception as exc:
            return {
                "success": False,
                "successMessage": "",
                "failureMessage": f"SAP HANA validation failed: {str(exc)}",
                "error": str(exc)
            } 