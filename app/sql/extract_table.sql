SELECT
    T.COMMENTS AS REMARKS,
    T.CREATE_TIME,
    'DEFAULT' AS TABLE_CAT,
    T.SCHEMA_NAME AS TABLE_SCHEM,
    T.TABLE_NAME,
    T.TABLE_OID,
    T.TABLE_TYPE,
    MT.IS_PARTITIONED AS HAS_PARTITIONS,
    MT.RECORD_COUNT AS ROW_COUNT,
    MT.TABLE_SIZE as BYTES,
    T.HAS_PRIMARY_KEY,
    T.IS_COLUMN_TABLE,
    T.IS_TEMPORARY,
    T.IS_USER_DEFINED_TYPE
FROM
    SYS.TABLES T
    LEFT JOIN SYS.M_TABLES MT
      ON MT.SCHEMA_NAME = T.SCHEMA_NAME
      AND MT.TABLE_NAME = T.TABLE_NAME
WHERE
    T.SCHEMA_NAME NOT IN (
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
    AND T.SCHEMA_NAME NOT LIKE 'SYS\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE '\_SYS\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE 'SYSHDL\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE 'SAP\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE '\_SAP\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE 'PAL\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE 'FDT\_%' ESCAPE '\'
    AND T.SCHEMA_NAME NOT LIKE 'XSSQLCC\_AUTO\_USER\_%' ESCAPE '\'
    AND T.TABLE_TYPE = 'TABLE'
    AND concat('DEFAULT', concat('.', T.SCHEMA_NAME)) !~ '{normalized_exclude_regex}'
    AND concat('DEFAULT', concat('.', T.SCHEMA_NAME)) ~ '{normalized_include_regex}'; 