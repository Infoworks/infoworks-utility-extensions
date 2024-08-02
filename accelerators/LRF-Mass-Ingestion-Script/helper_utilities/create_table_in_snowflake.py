import random
import pandas as pd
import numpy as np
import json
import snowflake.connector

ctx = snowflake.connector.connect(
          host="",
          user="",
          password="",
          account="",
          warehouse="",
          database="",
          schema="",
          protocol='')

# Create a cursor object.
cur = ctx.cursor()
# https://docs.snowflake.com/en/user-guide/python-connector-install.html#step-1-install-the-connector


choices_for_origcols = ['number', 'string', 'int', 'decimal(12,7)', 'decimal(3,1)']
choices_for_addncols = ['number', 'string', 'int', 'decimal(12,7)', 'decimal(3,1)', 'date', 'timestamp']

table_schema_path = "/Users/abhishek.raviprasad/Downloads/GitHub/ATT/fixed_width_automation/iwx/temp.csv"
# table_schema_path = "/Users/abhishek.raviprasad/Downloads/GitHub/mass-ingestion-script/fixed_width_automation/failed_tables_table_schema.csv"
#table_schema_path = "/Users/nitin.bs/PycharmProjects/mass-ingestion-script/fixed_width_automation/table_schema.csv"
try:
    table_schema_df = pd.read_csv(table_schema_path).replace(np.nan, None, regex=True)
except TypeError:
    table_schema_df = pd.read_csv(table_schema_path)

for index, row in table_schema_df.iterrows():
    try:
        TABLE_NAME = row["TABLE_NAME"].split(".")[-1]
        TABLE_SCHEMA_MAPPINGS = json.loads(row["TABLE_SCHEMA_MAPPINGS"])
        columns = TABLE_SCHEMA_MAPPINGS.keys()
        create_table_statement = f"create table \"ABHI_DATABASE\".\"PUBLIC\".\"{TABLE_NAME}\" ("

        for item in columns:
            if item.upper().endswith("DT") or item.upper().endswith("DATE"):
                dt = "DATE"
            else:
                dt = random.choice(choices_for_origcols)
            create_table_statement = create_table_statement + f"{item.strip()} {dt},"

        for item in json.loads(row["NEW_COLUMNS"]):
            col_name, datatype = list(item.items())[0]
            if datatype.lower() == "current_date":
                dt = "date"
            elif datatype.lower() == "current_time":
                dt = "int"
            else:
                dt = "string"
            create_table_statement = create_table_statement + f"{col_name.strip()} {dt},"
        for item in ["AR_ADDN_COL1", "AR_ADDN_COL2", "AR_ADDN_COL3"]:
            dt = random.choice(choices_for_addncols)
            create_table_statement = create_table_statement + f"{item.strip()} {dt},"

        create_table_statement = create_table_statement.strip(",") + ");"
        print(create_table_statement)
        cur.execute(create_table_statement)
    except Exception as e:
        print(str(e))

cur.close()
