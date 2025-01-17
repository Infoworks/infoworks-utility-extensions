#Usage:
#python3 teradata_metadata_analysis_with_watermark.py --host <your_teradata_host_ip> --port <teradata_port> --user <your_user> --password <your_encrypted_password> --schemas <td_schemas_space_seperated>

import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None
import teradatasql
import argparse
import json
import os
from utils import aes_decrypt,trim_spaces,target_schema_name,target_table_name,panda_strip,check_file_writable

#Establish the connection to the Teradata database
cwd=os.getcwd()
parser = argparse.ArgumentParser(description='Teradata metadata extraction')
parser.add_argument('--host',type=str,required=True,help='hostname/ip of teradata database')
parser.add_argument('--port',type=str,required=False,default="1025",help='port of teradata database(default 1025)')
parser.add_argument('--user',type=str,required=True,help='username for teradata database')
parser.add_argument('--password',type=str,required=True,help="encrypted password for teradata database and should not be enclosed in b''")
parser.add_argument('--schemas',nargs='*',default=[],help='Specify the schema to pull data from (space separated)')
parser.add_argument('--tables',nargs='*',default=[],help='Specify the tables to pull (space separated)')
parser.add_argument('--configuration_file_path',type=str,required=False,default=f"{cwd}/conf/configurations.json",help='Pass the absolute path to configuration file json.Default("./conf/configurations.json")')
parser.add_argument('--metadata_csv_path',type=str,required=False,default=f"{cwd}/csv/teradata_metadata.csv",help='Pass the absolute path to metadata csv file.Default("./csv/teradata_metadata.csv")')
args = parser.parse_args()
if not check_file_writable(args.metadata_csv_path):
    print("Pass the right metadata_csv_path including file_name.Exiting..")
    exit(-100)
if not os.path.exists(args.configuration_file_path) or not os.path.isfile(args.configuration_file_path):
    print("Either the configuration_file_path doesn't exist or is not a file.Exiting..")
    exit(-100)
host=args.host
port=args.port
user=args.user
encrypted_pass=args.password
config_file_path = args.configuration_file_path
metadata_csv_path=args.metadata_csv_path
password=aes_decrypt(encrypted_pass)
schema=args.schemas
tables=args.tables
#print("schema",schema)
schema=','.join("'" + schema_name + "'" for schema_name in list(map(str.upper,schema)))
tables=','.join("'" + table_name + "'" for table_name in list(map(str.upper,tables)))
#print("schema",schema)
schema_query = ""
tables_query=""
if(schema==""):
    schema_query = ""
else:
    schema_query = f"and DataBaseName in ({schema})"

if(tables==""):
    tables_query = ""
else:
    tables_query = f"and TableName in ({tables})"
connection = teradatasql.connect(host=host,dbs_port=port, user=user, password=password)
#query to fetch all tables size
#table_size_query = f"SELECT A.DataBaseName as DATABASENAME,A.TableName as TABLENAME,B.TableKind as TABLEKIND,CAST(SUM(A.currentperm)/(1024*1024) AS DECIMAL(18,5)) AS TABLE_SIZE_MB FROM dbc.tablesize as A right outer join DBC.TablesV as B on A.DataBaseName= B.DatabaseName and A.TableName=B.TableName where (B.TableKind = 'T' or B.TableKind = 'V') {schema_query} group by A.DataBaseName,A.TableName,B.TableKind";
table_size_query = f"Select DataBaseName as DATABASENAME,TableName as TABLENAME FROM DBC.TablesV where (TableKind = 'T' or TableKind = 'V') {schema_query} {tables_query} group by DataBaseName,TableName,TableKind"
print("table_size_query:",table_size_query)

#query to fetch the primary keys
if(schema==""):
    schema_query = ""
else:
    schema_query = f"and t.DataBaseName in ({schema})"
if(tables==""):
    tables_query = ""
else:
    tables_query = f"and t.TableName in ({tables})"

tables_primary_keys = f"""SELECT  t.DataBaseName as DATABASENAME,
        t.TableName as TABLENAME,
        i.IndexName as INDEXNAME,
        TRIM(TRAILING ','
            FROM XMLAGG(i.columnName || ','
                 ORDER BY i.columnPosition)(varchar(10000))) AS PROBABLE_NATURAL_KEY_COLUMNS
FROM    DBC.TablesV t
LEFT JOIN   DBC.IndicesV i
ON  t.DatabaseName = i.DataBaseName
AND t.TableName = i.TableName
AND (i.IndexType = 'K' or i.IndexType='P' or i.IndexType = 'A'  or i.IndexType = 'U'  or i.IndexType = 'Q' or i.IndexNumber=1)
WHERE   (t.TableKind = 'T' or t.TableKind = 'V')
AND t.DataBaseName NOT IN ('All', 'Crashdumps', 'DBC', 'dbcmngr',
    'Default', 'External_AP', 'EXTUSER', 'LockLogShredder', 'PUBLIC',
    'Sys_Calendar', 'SysAdmin', 'SYSBAR', 'SYSJDBC', 'SYSLIB',
    'SystemFe', 'SYSUDTLIB', 'SYSUIF', 'TD_SERVER_DB', 'TDStats',
    'TD_SYSGPL', 'TD_SYSXML', 'TDMaps', 'TDPUSER', 'TDQCD',
    'tdwm', 'SQLJ', 'TD_SYSFNLIB', 'SYSSPATIAL') {schema_query} {tables_query}
GROUP BY t.DatabaseName,
         t.TableName,
         i.IndexName
ORDER BY t.DatabaseName,
         t.TableName;"""


if(schema==""):
    schema_query = ""
else:
    schema_query = f"and DataBaseName in ({schema})"

if(tables==""):
    tables_query = ""
else:
    tables_query = f"and TableName in ({tables})"

split_by_keys=f"""SELECT  DataBaseName as DATABASENAME,
        TableName as TABLENAME,
        TRIM(TRAILING ',' FROM XMLAGG(ColumnName || ',')(varchar(10000))) AS SPLIT_BY_KEY_CANDIDATES
FROM    DBC.ColumnsV
WHERE   ColumnType in ('I1','I2','I8','I','DA', 'AT', 'TS', 'TZ', 'SZ', 'YR', 'YM', 'MO',
        'DY', 'DH', 'DM', 'DS', 'HR', 'HM', 'HS', 'MI', 'MS', 'SC')
        AND DataBaseName NOT IN ('All', 'Crashdumps', 'DBC', 'dbcmngr',
        'Default', 'External_AP', 'EXTUSER', 'LockLogShredder', 'PUBLIC',
        'Sys_Calendar', 'SysAdmin', 'SYSBAR', 'SYSJDBC', 'SYSLIB',
        'SystemFe', 'SYSUDTLIB', 'SYSUIF', 'TD_SERVER_DB',
        'TD_SYSGPL', 'TD_SYSXML', 'TDMaps', 'TDPUSER', 'TDQCD',
        'TDStats', 'tdwm', 'SQLJ', 'SYSSPATIAL','TD_SYSFNLIB') {schema_query} {tables_query}
GROUP BY DataBaseName,
            TableName
ORDER BY    DataBaseName,
            TableName;"""
#Fetch the data from Teradata using Pandas Dataframe
pd_table_size = pd.read_sql(table_size_query,connection)

pd_primary_keys = pd.read_sql(tables_primary_keys,connection)

pd_split_by_keys = pd.read_sql(split_by_keys,connection)
print(pd_split_by_keys)
pd_primary_keys=pd_primary_keys.apply(lambda x: panda_strip(x))
pd_table_size=pd_table_size.apply(lambda x: panda_strip(x))
pd_split_by_keys=pd_split_by_keys.apply(lambda x: panda_strip(x))

print("pd_table_size\n",pd_table_size.info(verbose=True))
#print(pd_table_size)
print("pd_primary_keys\n",pd_primary_keys.info(verbose=True))

#print(pd_primary_keys)
print("pd_split_by_keys\n",pd_split_by_keys.info(verbose=True))

resultant_temp = pd_table_size.merge(pd_primary_keys[['DATABASENAME','TABLENAME',"PROBABLE_NATURAL_KEY_COLUMNS"]],how="left")
resultant=resultant_temp.merge(pd_split_by_keys[['DATABASENAME','TABLENAME','SPLIT_BY_KEY_CANDIDATES']],how="left")
configuration_file= open(config_file_path,"r")
configuration_json=json.load(configuration_file)
#append_water_marks_columns=configuration_json['append_water_marks_columns']
#print(append_water_marks_columns)
#print(type(append_water_marks_columns))
#resultant['natural_keys_len_0?']=np.where(len(resultant['PROBABLE_NATURAL_KEY_COLUMNS'].tolist())!=0,True,False)
#resultant['append_columns_in_splitby']=np.where(len([value for value in configuration_json["append_water_marks_columns"] if value in resultant['SPLIT_BY_KEY_CANDIDATES'].tolist()])!=0,str(resultant['SPLIT_BY_KEY_CANDIDATES'].tolist()),str(resultant['SPLIT_BY_KEY_CANDIDATES'].tolist()))
resultant=resultant.fillna('')
resultant = resultant.assign(DERIVE_SPLIT_COLUMN_FUCTION='')
storage_format = configuration_json.get("ingestion_storage_format","parquet")
resultant=resultant.assign(STORAGE_FORMAT=storage_format)
resultant=resultant.assign(INGESTION_STRATEGY='')
for index,row in resultant.iterrows():
    resultant['INGESTION_STRATEGY'][index]=np.where(len(list(map(trim_spaces,resultant['PROBABLE_NATURAL_KEY_COLUMNS'][index].split(','))))!=0 and len(list(map(trim_spaces,[value for value in configuration_json["merge_water_marks_columns"] if value in list(map(trim_spaces,resultant['SPLIT_BY_KEY_CANDIDATES'][index].split(',')))])))!=0,'INCREMENTAL_APPEND','FULL_REFRESH')
for index,row in resultant.iterrows():
    resultant['INGESTION_STRATEGY'][index]=np.where(len(list(map(trim_spaces,resultant['PROBABLE_NATURAL_KEY_COLUMNS'][index].split(','))))!=0 and len(list(map(trim_spaces,[value for value in configuration_json["merge_water_marks_columns"] if value in list(map(trim_spaces,resultant['SPLIT_BY_KEY_CANDIDATES'][index].split(',')))])))!=0,'INCREMENTAL_MERGE','FULL_REFRESH')
resultant=resultant.assign(WATERMARK_COLUMN='')
resultant=resultant.assign(TPT_READER_INSTANCES='')
resultant=resultant.assign(TPT_WRITER_INSTANCES='')
resultant=resultant.assign(TPT_OR_JDBC='')
if "sfSchema" in configuration_json.keys():
    resultant['TARGET_SCHEMA_NAME']=configuration_json.get("sfSchema",resultant['DATABASENAME'].apply(target_schema_name))
elif "target_schema_name" in configuration_json.keys():
    resultant['TARGET_SCHEMA_NAME']=configuration_json.get("target_schema_name",resultant['DATABASENAME'].apply(target_schema_name))
else:
    resultant['TARGET_SCHEMA_NAME'] =resultant['DATABASENAME'].apply(target_schema_name)
resultant['TARGET_TABLE_NAME']=resultant['TABLENAME'].apply(target_table_name)
resultant=resultant.assign(TABLE_GROUP_NAME='')
resultant=resultant.assign(CONNECTION_QUOTA='')
resultant=resultant.assign(PARTITION_COLUMN='')
resultant=resultant.assign(DERIVED_PARTITION='False')
resultant=resultant.assign(DERIVED_FORMAT='')
resultant=resultant.assign(SCD_TYPE_2='False')
resultant=resultant.assign(TPT_WITHOUT_IWX_PROCESSING='False')
table_type = configuration_json.get("default_table_type","infoworks_managed_table")
resultant=resultant.assign(TABLE_TYPE=table_type)
resultant = resultant.assign(USER_MANAGED_TABLE_TARGET_PATH='')
for index,row in resultant.iterrows():
    temp=[value for value in configuration_json["merge_water_marks_columns"] if value in list(map(trim_spaces,[value for value in configuration_json["merge_water_marks_columns"] if value in list(map(trim_spaces,resultant['SPLIT_BY_KEY_CANDIDATES'][index].split(',')))]))] if resultant['INGESTION_STRATEGY'][index]=='INCREMENTAL_MERGE' else ''
    resultant['WATERMARK_COLUMN'][index]=",".join(temp)
#resultant['INCREMENTAL_STRATEGY']=resultant.apply(lambda x:'INCREMENTAL_MERGE' if len(list(x['PROBABLE_NATURAL_KEY_COLUMNS']))!=0 and len([value for value in configuration_json["append_water_marks_columns"] if value in x['SPLIT_BY_KEY_CANDIDATES'].split(',')])!=0 else 'FULL_REFRESH')
#for index,row in resultant.iterrows():
#    resultant['INCREMENTAL_STRATEGY'][index]='INCREMENTAL_MERGE' if len(list(resultant['PROBABLE_NATURAL_KEY_COLUMNS']))!=0 and len([value for value in configuration_json["append_water_marks_columns"] if value in row['SPLIT_BY_KEY_CANDIDATES'].split(',')])!=0 else 'FULL_REFRESH'
#for index,row in resultant.iterrows():
#    resultant['WATERMARK_COLUMN'][index]=str([value for value in configuration_json["append_water_marks_columns"] if value in row['SPLIT_BY_KEY_CANDIDATES'].split(',')]) if row['INCREMENTAL_STRATEGY']=='INCREMENTAL_MERGE' else ''
#print(resultant)

#print the rows in the table
#print(resultant)
#print(resultant.to_string(index=False))
apply_rtrim_to_strings = configuration_json.get("rtrim_string_columns",False)
resultant=resultant.assign(RTRIM_STRING_COLUMNS=apply_rtrim_to_strings)
resultant=resultant.assign(CUSTOM_TAGS='')
resultant=resultant.fillna('')
try:
    resultant.to_csv(f'{metadata_csv_path}',index=False)
    print(f"Please find the intermediate CSV file at {metadata_csv_path}")
except Exception as e:
    print(str(e))
    exit(0)
