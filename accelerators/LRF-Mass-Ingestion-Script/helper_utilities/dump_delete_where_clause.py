import pandas as pd

SCHEMA_LOOKUP = {"FINANCE": "SDW_AEDW_FINC_DB", "PRODUCTS": "SDW_AEDW_PRDS_DB",
                 "BILLING": "SDW_AEDW_BLNG_DB", "CUSTOMER": "SDW_AEDW_CUST_DB",
                 "SERVICE_ORDER": "SDW_AEDW_SORD_DB", "MARKETING": "SDW_AEDW_MKTG_DB",
                 "MR2000": "SDW_AEDW_MR2000_DB", "SDR_PROCESSES": "SDW_AEDW_SDR_PROCS_DB",
                 "NETWORK": "SDW_AEDW_NTWK_DB", "USAGE": "SDW_AEDW_USGE_DB"}

table_schema_path = "/fixed_width_automation/iwx/failed_tables_table_schema.csv"
table_schema_df = pd.read_csv(table_schema_path)

table_schema_df = table_schema_df[~table_schema_df['DELETE_WHERE_CLAUSE'].isna()]
table_schema_df = table_schema_df[~table_schema_df['SELECTIVE_INSERTS'].isna()]

df_temp = table_schema_df.groupby(['TABLE_NAME'], as_index=False).agg(
    {'DELETE_WHERE_CLAUSE': ' OR '.join, 'SELECTIVE_INSERTS': ' OR '.join})

df_temp["SELECTIVE_INSERTS"] = df_temp["SELECTIVE_INSERTS"].apply(lambda x: " OR ".join(list(set(x.split(" OR ")))))
# df_temp = df_temp.groupby(['TABLE_NAME'])['SELECTIVE_INSERTS'].apply(' OR '.join)

with open("../tables_with_delete_statements.csv", "w") as f:
    f.write("TABLE_NAME,DELETE_WHERE_CLAUSE,SELECTIVE_INSERTS")
    f.write("\n")
    for index, row in df_temp.iterrows():
        SCHEMA, TABLE_NAME = row["TABLE_NAME"].split(".")
        SCHEMA = SCHEMA_LOOKUP.get(SCHEMA, SCHEMA)
        FINAL_TABLE_NAME = SCHEMA + "." + TABLE_NAME
        f.write(FINAL_TABLE_NAME + "," + row["DELETE_WHERE_CLAUSE"] + "," + row["SELECTIVE_INSERTS"])
        f.write("\n")
