SELECT
    'DEFAULT' as PROCEDURE_CAT,
    SCHEMA_NAME as PROCEDURE_SCHEM,
    SCHEMA_NAME as TABLE_SCHEM,
    PROCEDURE_NAME,
    PROCEDURE_TYPE,
    CREATE_TIME,
    CAST(DEFINITION AS NVARCHAR) as PROCEDURE_DEFINITION,
    PROCEDURE_OID,
    COMMENTS as REMARKS,
    IS_VALID,
    SQL_SECURITY,
    IS_BUILTIN
FROM
    SYS.PROCEDURES
WHERE
    SCHEMA_NAME NOT IN (
        'SYSTEM',
        'SYS',
        'SYSHDL',
        'BROKER_PO_USER',
        'BROKER_USER',
        'HANA_XS_BASE',
        'UIS',
        'PUBLIC',
        'SAPHANADB',
        'DBACOCKPIT',
        'SAPDBCTRL'
    )
    AND SCHEMA_NAME NOT LIKE 'SYS\\_%' ESCAPE '\\'
    AND SCHEMA_NAME NOT LIKE '\\_SYS\\_%' ESCAPE '\\'
    AND SCHEMA_NAME NOT LIKE 'SYSHDL\\_%' ESCAPE '\\'
    AND SCHEMA_NAME NOT LIKE 'SAP\\_%' ESCAPE '\\'
    AND SCHEMA_NAME NOT LIKE '\\_SAP\\_%' ESCAPE '\\'
    AND SCHEMA_NAME NOT LIKE 'PAL\\_%' ESCAPE '\\'
    AND SCHEMA_NAME NOT LIKE 'FDT\\_%' ESCAPE '\\'
    AND SCHEMA_NAME NOT LIKE 'XSSQLCC\\_AUTO\\_USER\\_%' ESCAPE '\\'
    AND concat('DEFAULT', concat('.', SCHEMA_NAME)) !~ '{normalized_exclude_regex}'
    AND concat('DEFAULT', concat('.', SCHEMA_NAME)) ~ '{normalized_include_regex}'; 