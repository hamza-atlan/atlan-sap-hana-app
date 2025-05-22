-- Extract calculation view column information from SAP HANA
SELECT 
    'DEFAULT' AS TABLE_CAT,
    sys_views.SCHEMA_NAME AS TABLE_SCHEM,
    active_object.PACKAGE_ID AS PACKAGE_ID,
    active_object.OBJECT_NAME AS VIEW_NAME,
    bimc_properties.COLUMN_NAME,
    bimc_properties.COLUMN_SQL_TYPE
FROM
    _SYS_REPO.ACTIVE_OBJECT active_object 
    JOIN 
    _SYS_BI.BIMC_PROPERTIES bimc_properties ON active_object.OBJECT_NAME = bimc_properties.CUBE_NAME
    JOIN
    SYS.VIEWS sys_views ON concat(concat(active_object.PACKAGE_ID,'/'),active_object.OBJECT_NAME) = sys_views.VIEW_NAME  
WHERE
    active_object.OBJECT_SUFFIX = 'calculationview' 
    AND bimc_properties.COLUMN_FLAG = 'Dimension Attribute' 