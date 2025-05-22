SELECT 
    COUNT(*) AS TABLE_COUNT
FROM 
    SYS.TABLES T
WHERE 
    T.SCHEMA_NAME NOT IN (
        'SYSTEM', 
        'SYS',
        'SYSHDL',
        'BROKER_PO_USER',
        'BROKER_USER',
        'HANA_XS_BASE',
        'UIS',
        'PUBLIC'
    )
    AND T.SCHEMA_NAME NOT LIKE 'SYS\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE '\_SYS\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE 'SYSHDL\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE 'SAP\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE '\_SAP\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE 'PAL\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE 'FDT\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE 'XSSQLCC\_AUTO\_USER\_%' ESCAPE '\'
    AND concat(current_schema, concat('.', T.SCHEMA_NAME)) !~ '{normalized_exclude_regex}'
    AND concat(current_schema, concat('.', T.SCHEMA_NAME)) ~ '{normalized_include_regex}'; 