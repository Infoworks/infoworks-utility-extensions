#Usage:
#source $IW_HOME/bin/env.sh
#python -m pip install -r requirements.txt
#python netezza_metadata_analysis_with_watermark.py --host <your_netezza_host_ip> --port <netezza_port> --user <your_user> --password <your_encrypted_password> --db <database name> --schemas <netezza_schemas_space_seperated>

import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None
import argparse
import inspect
import sys
import json
import os
import subprocess
import pkg_resources
script_name = inspect.getfile(inspect.currentframe())
# set infoworks python path in order to allow imports
current_dir = os.path.dirname(os.path.abspath(script_name))
start_iw_services_dir = os.path.abspath(os.path.join(current_dir, '..'))
start_iw_services_libs_dir = os.path.abspath(
    os.path.join(start_iw_services_dir, 'resources', 'python-libs'))

script_location = os.path.dirname(os.path.realpath(__file__))
cwd = os.getcwd()

def trim_spaces(item):
    return item.strip()

def target_table_name(table_name):
    return table_name

def target_schema_name(table_schema):
    return table_schema

def get_iw_base_dir():
    try:
        return os.environ['IW_HOME']
    except KeyError as e:
        try:
            iw_home = os.path.abspath(script_location + "/../../")
            print("Infoworks home is as follows", iw_home)
            os.environ['IW_HOME'] = iw_home
            return os.environ['IW_HOME']
        except KeyError as e:
            raise RuntimeError(
                'IW_HOME is not set as an env variable. Source {IW_HOME}/bin/env.sh before running this.'
            )
        except Exception as e:
            raise RuntimeError(e)
    except Exception as e:
        raise RuntimeError(e)

sys.path.insert(0, start_iw_services_dir)
sys.path.insert(0, "{}/apricot-meteor/infoworks_python".format(get_iw_base_dir()))

sys.path.insert(1, start_iw_services_libs_dir)
iw_home=get_iw_base_dir()

required = {'JayDeBeApi==1.2.3'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed

if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
import jaydebeapi

try:
    from infoworks.core.iw_utils import IWUtils
    from infoworks.core.mongo_utils import mongodb
except Exception as e:
    print(e)
    sys.exit(1)

def panda_strip(x):
    r =[]
    for y in x:
        if isinstance(y, str):
            y = y.strip()

        r.append(y)
    return pd.Series(r)

#Establish the connection to the Netezza database
parser = argparse.ArgumentParser(description='Netezza metadata extraction')
parser.add_argument('--host',type=str,required=True,help='hostname/ip of Netezza database')
parser.add_argument('--port',type=str,required=False,default="5480",help='port of Netezza database(default 5480)')
parser.add_argument('--user',type=str,required=True,help='username for Netezza database')
parser.add_argument('--password',type=str,required=True,help="encrypted password for Netezza database and should not be enclosed in b''")
parser.add_argument('--db',type=str,required=True,help='database name in Netezza')
parser.add_argument('--driver_path',type=str,default=[],help='Specify the absolute driver jar file path along with filename')
parser.add_argument('--schemas',nargs='*',default=[],help='Specify the schema to pull data from')

args = parser.parse_args()

host=args.host
port=args.port
user=args.user
encrypted_pass=args.password
driver_path=args.driver_path

password=subprocess.check_output(["sh",f"{iw_home}/apricot-meteor/infoworks_python/infoworks/bin/infoworks_security.sh","-decrypt","-p",encrypted_pass]).decode("utf-8").strip("\n")
schema=args.schemas
#print("schema",schema)
schema=','.join("'" + schema_name + "'" for schema_name in list(map(str.upper,schema)))
#print("schema",schema)
schema_query = ""

dsn_database = args.db
dsn_hostname = host
dsn_port = port
dsn_uid = user
dsn_pwd = password
jdbc_driver_name = "org.netezza.Driver"
jdbc_driver_loc = os.path.join(driver_path)
connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database

if(schema==""):
    schema_query = ""
else:
    schema_query = f" schema in ({schema}) and owner not in ('ADMIN')"

def listagg(L):
    return ",".join(L)

url = '{0}:user={1};password={2}'.format(connection_string, dsn_uid, dsn_pwd)
print("Connection String: " + connection_string)
connection = jaydebeapi.connect(jdbc_driver_name, connection_string, {'user': dsn_uid, 'password': dsn_pwd},jars=jdbc_driver_loc)
#query to fetch all tables size
table_size_query = f"""
select
schema as DATABASENAME,
TABLENAME,
used_bytes/pow(1024,3) as TABLE_SIZE_IN_GB
from _v_table_storage_stat
where {schema_query}"""


#query to fetch the primary keys
if(schema==""):
    schema_query = ""
else:
    schema_query = f"and schema in ({schema})"

tables_primary_keys = f"""
SELECT  schema as DATABASENAME
        , constraintname
        , relation as TABLENAME
        , conseq as seq
        , attname as PROBABLE_NATURAL_KEY_COLUMNS
FROM _v_relation_keydata
where (contype='p' or contype='u') {schema_query}
order by relation, conseq
"""


if(schema==""):
    schema_query = ""
else:
    schema_query = f"and schema in ({schema})"


split_by_keys=f"""
select
schema as DATABASENAME,table_name as TABLENAME,column_name as SPLIT_BY_KEY_CANDIDATES
from _V_SYS_COLUMNS
where TYPE_NAME in ('TIMESTAMP','DATE') {schema_query}
"""
#Fetch the data from Teradata using Pandas Dataframe
pd_table_size = pd.read_sql(table_size_query,connection)

pd_primary_keys = pd.read_sql(tables_primary_keys,connection)
#print(pd_primary_keys)
pd_primary_keys=pd_primary_keys.groupby(['TABLENAME','DATABASENAME'])['PROBABLE_NATURAL_KEY_COLUMNS'].apply(listagg).reset_index(name="PROBABLE_NATURAL_KEY_COLUMNS")
#print(pd_primary_keys)
if pd_primary_keys.empty:
    pd_primary_keys=pd.DataFrame(columns = ['TABLENAME','DATABASENAME','PROBABLE_NATURAL_KEY_COLUMNS'])

pd_split_by_keys = pd.read_sql(split_by_keys,connection)
pd_split_by_keys=pd_split_by_keys.groupby(['TABLENAME','DATABASENAME'])['SPLIT_BY_KEY_CANDIDATES'].apply(listagg).reset_index(name="SPLIT_BY_KEY_CANDIDATES")

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
configuration_file= open(f"{cwd}/conf/configurations.json","r")
configuration_json=json.load(configuration_file)
#append_water_marks_columns=configuration_json['append_water_marks_columns']
#print(append_water_marks_columns)
#print(type(append_water_marks_columns))
#resultant['natural_keys_len_0?']=np.where(len(resultant['PROBABLE_NATURAL_KEY_COLUMNS'].tolist())!=0,True,False)
#resultant['append_columns_in_splitby']=np.where(len([value for value in configuration_json["append_water_marks_columns"] if value in resultant['SPLIT_BY_KEY_CANDIDATES'].tolist()])!=0,str(resultant['SPLIT_BY_KEY_CANDIDATES'].tolist()),str(resultant['SPLIT_BY_KEY_CANDIDATES'].tolist()))
resultant=resultant.fillna('')
resultant=resultant.assign(STORAGE_FORMAT='parquet')
resultant=resultant.assign(INGESTION_STRATEGY='')

for index,row in resultant.iterrows():
    resultant['INGESTION_STRATEGY'][index]=np.where(len(list(map(trim_spaces,resultant['PROBABLE_NATURAL_KEY_COLUMNS'][index].split(','))))!=0 and len(list(map(trim_spaces,[value for value in configuration_json["merge_water_marks_columns"] if value in list(map(trim_spaces,resultant['SPLIT_BY_KEY_CANDIDATES'][index].split(',')))])))!=0,'INCREMENTAL_APPEND','FULL_REFRESH')
for index,row in resultant.iterrows():
    resultant['INGESTION_STRATEGY'][index]=np.where(len(list(map(trim_spaces,resultant['PROBABLE_NATURAL_KEY_COLUMNS'][index].split(','))))!=0 and len(list(map(trim_spaces,[value for value in configuration_json["merge_water_marks_columns"] if value in list(map(trim_spaces,resultant['SPLIT_BY_KEY_CANDIDATES'][index].split(',')))])))!=0,'INCREMENTAL_MERGE','FULL_REFRESH')
resultant=resultant.assign(WATERMARK_COLUMN='')
resultant=resultant.assign(SCD_TYPE_2='False')
resultant['TARGET_SCHEMA_NAME']=resultant['DATABASENAME'].apply(target_schema_name)
resultant['TARGET_TABLE_NAME']=resultant['TABLENAME'].apply(target_table_name)
resultant=resultant.assign(TABLE_GROUP_NAME='')
resultant=resultant.assign(CONNECTION_QUOTA='')
resultant=resultant.assign(PARTITION_COLUMN='')
resultant=resultant.assign(DERIVED_PARTITION='False')
resultant=resultant.assign(DERIVED_FORMAT='')

for index,row in resultant.iterrows():
    resultant['WATERMARK_COLUMN'][index]=[value for value in configuration_json["merge_water_marks_columns"] if value in list(map(trim_spaces,[value for value in configuration_json["merge_water_marks_columns"] if value in list(map(trim_spaces,resultant['SPLIT_BY_KEY_CANDIDATES'][index].split(',')))]))][0] if resultant['INGESTION_STRATEGY'][index]=='INCREMENTAL_MERGE' else ''
#resultant['INCREMENTAL_STRATEGY']=resultant.apply(lambda x:'INCREMENTAL_MERGE' if len(list(x['PROBABLE_NATURAL_KEY_COLUMNS']))!=0 and len([value for value in configuration_json["append_water_marks_columns"] if value in x['SPLIT_BY_KEY_CANDIDATES'].split(',')])!=0 else 'FULL_REFRESH')
#for index,row in resultant.iterrows():
#    resultant['INCREMENTAL_STRATEGY'][index]='INCREMENTAL_MERGE' if len(list(resultant['PROBABLE_NATURAL_KEY_COLUMNS']))!=0 and len([value for value in configuration_json["append_water_marks_columns"] if value in row['SPLIT_BY_KEY_CANDIDATES'].split(',')])!=0 else 'FULL_REFRESH'
#for index,row in resultant.iterrows():
#    resultant['WATERMARK_COLUMN'][index]=str([value for value in configuration_json["append_water_marks_columns"] if value in row['SPLIT_BY_KEY_CANDIDATES'].split(',')]) if row['INCREMENTAL_STRATEGY']=='INCREMENTAL_MERGE' else ''
#print(resultant)

#print the rows in the table
#print(resultant)
#print(resultant.to_string(index=False))
resultant=resultant.fillna('')
try:
    resultant.to_csv(f'{iw_home}/temp/netezza_metadata.csv',index=False)
    print(f"Please find the intermediate CSV file at {iw_home}/temp/netezza_metadata.csv")
except Exception as e:
    print(str(e))
    exit(0)


