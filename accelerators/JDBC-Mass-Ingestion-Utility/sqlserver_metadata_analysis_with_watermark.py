import os
import json
import base64
import argparse
import numpy as np
import pandas as pd
import pymssql
pd.options.mode.chained_assignment = None
from utils import aes_decrypt,trim_spaces,target_schema_name,target_table_name,panda_strip,check_file_writable
cwd = os.getcwd()
parser = argparse.ArgumentParser(description='SQL Server metadata extraction')
parser.add_argument('--host', type=str, required=True, help='hostname/ip of SQL Server database')
parser.add_argument('--port', type=str, required=False, default="1433",
                    help='port of SQL Server database(default 1025)')
parser.add_argument('--user', type=str, required=True, help='username for SQL Server database')
parser.add_argument('--password', type=str, required=True,
                    help="encrypted password for SQL Server database and should not be enclosed in b''")
parser.add_argument('--database', type=str, required=True, help='Specify the database to pull data from')
parser.add_argument('--schemas', nargs='*', default=[], help='Specify the schema to pull data from (space separated)')
parser.add_argument('--tables', nargs='*', default=[], help='Specify the tables to pull (space separated)')
parser.add_argument('--configuration_file_path', type=str, required=False, default=f"{cwd}/conf/configurations.json",
                    help='Pass the absolute path to configuration file json.Default("./conf/configurations.json")')
parser.add_argument('--metadata_csv_path', type=str, required=False, default=f"{cwd}/csv/sqlserver_metadata.csv",
                    help='Pass the absolute path to metadata csv file.Default("./csv/sqlserver_metadata.csv")')
args = parser.parse_args()

if not check_file_writable(args.metadata_csv_path):
    print("Pass the right metadata_csv_path including file_name.Exiting..")
    exit(-100)
if not os.path.exists(args.configuration_file_path) or not os.path.isfile(args.configuration_file_path):
    print("Either the configuration_file_path doesn't exist or is not a file.Exiting..")
    exit(-100)

host = args.host
port = args.port
user = args.user
encrypted_pass = args.password
database = args.database
config_file_path = args.configuration_file_path
metadata_csv_path = args.metadata_csv_path
password = aes_decrypt(encrypted_pass)
schema = args.schemas
tables = args.tables

schema = ','.join("'" + schema_name + "'" for schema_name in list(map(str.upper, schema)))
tables = ','.join("'" + table_name + "'" for table_name in list(map(str.upper, tables)))

schema_query = "" if schema == "" else f"and TABLE_SCHEMA in ({schema})"
tables_query = "" if tables == "" else f"and TABLE_NAME in ({tables})"

connection = pymssql.connect(server=host, user=user, password=password, database=database)

table_schema_query = f'''
Select TABLE_SCHEMA AS DATABASENAME, TABLE_NAME AS TABLENAME  from INFORMATION_SCHEMA.TABLES
WHERE (TABLE_TYPE = 'BASE TABLE' OR TABLE_TYPE = 'VIEW') {schema_query} {tables_query}
GROUP  by TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE '''

# query to fetch the primary keys
schema_query = "" if schema == "" else f'and "TABLE_SCHEMA" in ({schema})'
tables_query = "" if tables == "" else f'and "TABLE_NAME" in ({tables})'

tables_primary_keys = f'''
SELECT t.TABLE_SCHEMA AS DATABASENAME, t.TABLE_NAME AS TABLENAME,
STUFF((
    SELECT ', ' + COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
    WHERE kcu.TABLE_SCHEMA=t.TABLE_SCHEMA  and kcu.TABLE_NAME = t.TABLE_NAME
    FOR XML PATH(''), TYPE
).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS PROBABLE_NATURAL_KEY_COLUMNS
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
WHERE CONSTRAINT_TYPE='PRIMARY KEY' {schema_query} {tables_query};
'''

schema_query = "" if schema == "" else f"and t.TABLE_SCHEMA in ({schema})"
tables_query = "" if tables == "" else f"and t.TABLE_NAME in ({tables})"

split_by_keys = f"""
SELECT 
t.TABLE_SCHEMA AS DATABASENAME,
t.TABLE_NAME AS TABLENAME,
STUFF(( SELECT ', ' + COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE DATA_TYPE IN ('int','bigint','datetime','datetime2','decimal','float','numeric') AND c.TABLE_NAME=t.TABLE_NAME and c.TABLE_SCHEMA=t.TABLE_SCHEMA
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS SPLIT_BY_KEY_CANDIDATES
FROM INFORMATION_SCHEMA.TABLES t 
WHERE t.TABLE_CATALOG='{database}' {schema_query} {tables_query}
group by t.TABLE_SCHEMA,t.TABLE_NAME 
order by t.TABLE_SCHEMA,t.TABLE_NAME;"""

pd_table_size = pd.read_sql(table_schema_query, connection)
pd_primary_keys = pd.read_sql(tables_primary_keys, connection)
pd_split_by_keys = pd.read_sql(split_by_keys, connection)
pd_primary_keys = pd_primary_keys.apply(lambda x: panda_strip(x))
pd_table_size = pd_table_size.apply(lambda x: panda_strip(x))
pd_split_by_keys = pd_split_by_keys.apply(lambda x: panda_strip(x))

print("pd_table_size\n", pd_table_size.info(verbose=True))
print("pd_primary_keys\n", pd_primary_keys.info(verbose=True))
print("pd_split_by_keys\n", pd_split_by_keys.info(verbose=True))

resultant_temp = pd_table_size.merge(pd_primary_keys[['DATABASENAME', 'TABLENAME', "PROBABLE_NATURAL_KEY_COLUMNS"]],
                                     how="left")
resultant = resultant_temp.merge(pd_split_by_keys[['DATABASENAME', 'TABLENAME', 'SPLIT_BY_KEY_CANDIDATES']], how="left")

configuration_file = open(config_file_path, "r")
configuration_json = json.load(configuration_file)

resultant = resultant.fillna('')
resultant = resultant.assign(DERIVE_SPLIT_COLUMN_FUCTION='')
storage_format = configuration_json.get("ingestion_storage_format", "parquet")
resultant = resultant.assign(STORAGE_FORMAT=storage_format)
resultant = resultant.assign(INGESTION_STRATEGY='')
for index, row in resultant.iterrows():
    resultant['INGESTION_STRATEGY'][index] = np.where(
        len(list(map(trim_spaces, resultant['PROBABLE_NATURAL_KEY_COLUMNS'][index].split(',')))) != 0 and len(list(
            map(trim_spaces, [value for value in configuration_json["append_water_marks_columns"] if value in list(
                map(trim_spaces, resultant['SPLIT_BY_KEY_CANDIDATES'][index].split(',')))]))) != 0,
        'INCREMENTAL_APPEND', 'FULL_REFRESH')
for index, row in resultant.iterrows():
    resultant['INGESTION_STRATEGY'][index] = np.where(
        len(list(map(trim_spaces, resultant['PROBABLE_NATURAL_KEY_COLUMNS'][index].split(',')))) != 0 and len(list(
            map(trim_spaces, [value for value in configuration_json["merge_water_marks_columns"] if value in list(
                map(trim_spaces, resultant['SPLIT_BY_KEY_CANDIDATES'][index].split(',')))]))) != 0, 'INCREMENTAL_MERGE',
        'FULL_REFRESH')
resultant = resultant.assign(WATERMARK_COLUMN='')
resultant = resultant.assign(TPT_READER_INSTANCES='')
resultant = resultant.assign(TPT_WRITER_INSTANCES='')
resultant = resultant.assign(TPT_OR_JDBC='')
if "sfSchema" in configuration_json.keys():
    resultant['TARGET_SCHEMA_NAME'] = configuration_json.get("sfSchema",
                                                             resultant['DATABASENAME'].apply(target_schema_name))
elif "target_schema_name" in configuration_json.keys():
    resultant['TARGET_SCHEMA_NAME'] = configuration_json.get("target_schema_name",
                                                             resultant['DATABASENAME'].apply(target_schema_name))
else:
    resultant['TARGET_SCHEMA_NAME'] = resultant['DATABASENAME'].apply(target_schema_name)
resultant['TARGET_TABLE_NAME'] = resultant['TABLENAME'].apply(target_table_name)
resultant = resultant.assign(TABLE_GROUP_NAME='')
resultant = resultant.assign(CONNECTION_QUOTA='')
resultant = resultant.assign(PARTITION_COLUMN='')
resultant = resultant.assign(DERIVED_PARTITION='False')
resultant = resultant.assign(DERIVED_FORMAT='')
resultant = resultant.assign(SCD_TYPE_2='False')
resultant = resultant.assign(TPT_WITHOUT_IWX_PROCESSING='False')
table_type = configuration_json.get("default_table_type", "infoworks_managed_table")
resultant = resultant.assign(TABLE_TYPE=table_type)
resultant = resultant.assign(USER_MANAGED_TABLE_TARGET_PATH='')
resultant = resultant.assign(CUSTOM_TAGS='')
for index, row in resultant.iterrows():
    if resultant['INGESTION_STRATEGY'][index] in ["INCREMENTAL_MERGE", "INCREMENTAL_APPEND"]:
        merge_watermark = [value for value in configuration_json["merge_water_marks_columns"] if
                           value in list(
                               map(trim_spaces, [value for value in configuration_json["merge_water_marks_columns"]
                                                 if value in list(
                                       map(trim_spaces, resultant['SPLIT_BY_KEY_CANDIDATES'][index].split(',')))]))] if \
            resultant['INGESTION_STRATEGY'][index] == 'INCREMENTAL_MERGE' else ''
        append_watermark = [value for value in configuration_json["append_water_marks_columns"] if
                            value in list(
                                map(trim_spaces, [value for value in configuration_json["append_water_marks_columns"]
                                                  if value in list(
                                        map(trim_spaces, resultant['SPLIT_BY_KEY_CANDIDATES'][index].split(',')))]))] if \
            resultant['INGESTION_STRATEGY'][index] == 'INCREMENTAL_APPEND' else ''

        resultant['WATERMARK_COLUMN'][index] = ",".join(merge_watermark) if merge_watermark != '' else append_watermark

resultant = resultant.fillna('')
try:
    resultant.to_csv(f'{metadata_csv_path}', index=False)
    print(f"Please find the intermediate CSV file at {metadata_csv_path}")
except Exception as e:
    print(str(e))
    exit(0)
