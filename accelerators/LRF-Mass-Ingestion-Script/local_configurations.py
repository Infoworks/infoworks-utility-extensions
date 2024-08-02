MAX_WORKERS = 10
SCHEMA_LOOKUP = {"FINANCE": "SDW_AEDW_FINC_DB", "PRODUCTS": "SDW_AEDW_PRDS_DB",
                 "BILLING": "SDW_AEDW_BLNG_DB", "CUSTOMER": "SDW_AEDW_CUST_DB",
                 "SERVICE_ORDER": "SDW_AEDW_SORD_DB", "MARKETING": "SDW_AEDW_MKTG_DB",
                 "MR2000": "SDW_AEDW_MR2000_DB", "SDR_PROCESSES": "SDW_AEDW_SDR_PROCS_DB",
                 "NETWORK": "SDW_AEDW_NTWK_DB", "USAGE": "SDW_AEDW_USGE_DB",
                 "SDR_PROCESSES_TMP":"PUBLIC","NETWORK_ABC": "PUBLIC"}
FIXED_WIDTH_SOURCE_EXTENSION_NAME = "att_source_extensions"
CSV_SOURCE_EXTENSION_NAME = "att_source_extensions"
UDF_INPUT_SQL_TYPE_MAPPING = {"DivideHiveUDF": 12, "CaseWhenIsNullThenEmpty": 12,
                              "CastAsdateHiveUDF": 12, "CastDateFormatElseAnotherDateHiveUDF": 12,
                              "CastNullIFtoDateFormatHiveUDF": 12, "CastNUllIFtoTimestamp0HiveUDF": 12,
                              "CastReturnCurrentTimestampIfNullElseGivenTimestampHiveUDF": 12,
                              "CastStringToTimestampFormatHiveUDF": 12, "caswhen_multiplethen_HiveUDF": 12,
                              "CoalesceAsCharHiveUDF": 12, "CoalesceCastToTimestamp0HiveUDF": 12, "CoalesceHiveUDF": 12,
                              "CoalesceNullIFHiveUDF": 12, "CoalesceTrimHiveUDF": 12, "DecimalCleanupHiveUDF": 12,
                              "NullCheckWithAsDecimalHiveUDF": 12, "NullIFHiveUDF": 12, "ReturnsecondinputHiveUDF": 12,
                              "ReturnZeroIfParamsEqualHiveUDF": 4, "StringtoDateFormatHiveUDF": 12,
                              "StringToTimestampHiveUDF": 12, "SubstrCharlengthTrimTrailHiveUDF": 12,
                              "SubstrHiveUDF": 12}
column_val_datatype_mapping = {12: "\"\"", 4: "123", -5: "123L", 3: "123.45", 7: "123.45", 8: "123.45BD",91: "to_date('9999-01-01','yyyy-MM-dd')", 93: "to_timestamp('9999-01-01 00:00:00','yyyy-MM-dd HH:mm:ss')"}
nvl2_datatype_mapping = {12: "\"\"", 4: "0", -5: "0", 3: "0", 7: "0", 8: "0", 91: "to_date('9999-01-01','yyyy-MM-dd')", 93: "to_timestamp('9999-01-01 00:00:00','yyyy-MM-dd HH:mm:ss')"}
