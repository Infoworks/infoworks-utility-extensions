# Usage:
# python3 mysql_metadata_analysis.py --host <your_mysql_host_ip> --user <your_user> --password <your_password> --schemas <space seperated list of schemas>
import pandas as pd

pd.options.mode.chained_assignment = None
import numpy as np
import sys
import inspect
import os
import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import pkg_resources
from utils import aes_decrypt,trim_spaces,target_schema_name,target_table_name,panda_strip,check_file_writable
required = {'pycryptodomex', 'pymysql'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed

if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
import pymysql


cwd = os.getcwd()


# Establish the connection to the MySql database

def main():
    parser = argparse.ArgumentParser(description='MySql metadata extraction')
    parser.add_argument('--host', type=str, required=True, help='hostname/ip of MySql database')
    parser.add_argument('--port', type=int, required=False, default=3306, help='port of MySql database')
    parser.add_argument('--user', type=str, required=True, help='username for MySql database')
    parser.add_argument('--password', type=str, required=True, help='password for MySql database')
    parser.add_argument('--schemas', nargs='*', default=[], help='Specify the schema to pull data from')
    parser.add_argument('--tables', nargs='*', default=[], help='Specify the tables to pull (space separated)')
    parser.add_argument('--configuration_file_path', type=str, required=False,
                        default=f"{cwd}/conf/configurations.json",
                        help='Pass the absolute path to configuration file json.Default("./conf/configurations.json")')
    parser.add_argument('--metadata_csv_path', type=str, required=False, default=f"{cwd}/csv/mysql_metadata.csv",
                        help='Pass the absolute path to metadata csv file.Default("./csv/mysql_metadata.csv")')
    parser.add_argument('--skip_views', type=str, choices=["True", "False"], required=False, default="False",
                        help='bool value to skip views if set to true')
    args = parser.parse_args()
    if not check_file_writable(args.metadata_csv_path):
        print("Pass the right metadata_csv_path including file_name.Exiting..")
        exit(-100)
    if not os.path.exists(args.configuration_file_path) or not os.path.isfile(args.configuration_file_path):
        print("Either the configuration_file_path doesn't exist or is not a file.Exiting..")
        exit(-100)
    # print(args)
    host = args.host
    port = args.port
    user = args.user
    encrypted_pass = args.password
    password = aes_decrypt(encrypted_pass)
    schema = args.schemas
    config_file_path = args.configuration_file_path
    metadata_csv_path = args.metadata_csv_path
    schema = ','.join("'" + schema_name + "'" for schema_name in list(map(str, schema)))
    print("schema", schema)
    tables = args.tables
    schema_query = ""
    skip_views = eval(args.skip_views)
    configuration_file = open(config_file_path, "r")
    configuration_json = json.load(configuration_file)
    view_query = ""
    if not skip_views:
        view_query = " or tab.table_type='SYSTEM VIEW'"
    if (schema == ""):
        schema_query = ""
    else:
        schema_query = f"and tab.table_schema IN ({schema})"
    ssl = {'ssl_verify_identity': 'true'}
    connection = pymysql.connect(host=host, port=port, user=user, passwd=password, connect_timeout=5, ssl=ssl)

    all_tables = f"""select table_schema as DATABASENAME,
    table_name as TABLENAME,tab.table_type as TABLE_OR_VIEW from information_schema.tables tab
    where (tab.table_type = 'BASE TABLE'{view_query})
    {schema_query}
    order by tab.table_schema, tab.table_name;"""
    if (schema == ""):
        schema_query = ""
    else:
        schema_query = f"and  tab.table_schema IN ({schema})"

    table_primary_keys = f"""select tab.table_schema as DATABASENAME,
    tab.table_name as TABLENAME,
    group_concat(distinct sta.column_name order by sta.column_name) as PROBABLE_NATURAL_KEY_COLUMNS
    from information_schema.tables as tab
    inner join information_schema.statistics as sta
    	on sta.table_schema = tab.table_schema
        and sta.table_name = tab.table_name
        and sta.index_name = 'primary'
    where (tab.table_type = 'BASE TABLE'{view_query}) 
    {schema_query}
    group by tab.table_name,tab.table_schema
    order by tab.table_name,tab.table_schema;"""
    primary_key_df = pd.read_sql(table_primary_keys, con=connection)
    primary_key_df.set_index(['TABLENAME', 'DATABASENAME'], drop=True)
    # print(df.to_string(index=False))
    primary_key_df.reset_index(drop=True)
    print("primary_key_df\n", primary_key_df.info(verbose=True))
    schema_query = ""
    if (schema == ""):
        schema_query = ""
    else:
        schema_query = f"and  tab.table_schema IN ({schema})"

    split_by_key = f"""select tab.table_schema as DATABASENAME,
    tab.table_name as TABLENAME,
    group_concat(distinct col.column_name) as SPLIT_BY_KEY_CANDIDATES
    from information_schema.tables as tab
    inner join information_schema.columns as col
        on col.table_schema = tab.table_schema
        and col.table_name = tab.table_name
    where (tab.table_type = 'BASE TABLE'{view_query})
    {schema_query}
    and col.data_type in ('date','timestamp','int')
    group by tab.table_schema,tab.table_name
    order by tab.table_schema,tab.table_name"""
    split_by_key_df = pd.read_sql(split_by_key, con=connection)
    split_by_key_df.reset_index(drop=True)
    split_by_key_df.set_index(['TABLENAME', 'DATABASENAME'], drop=True)
    print("split_by_key_df\n", split_by_key_df.info(verbose=True))
    # print(split_by_key_df.columns)
    all_tables_df = pd.read_sql(all_tables, con=connection)
    all_tables_df.reset_index(drop=True)
    all_tables_df.set_index(['TABLENAME', 'DATABASENAME'], drop=True)
    print("all_tables_df\n", all_tables_df.info())
    primary_key_df = all_tables_df.merge(primary_key_df[['DATABASENAME', 'TABLENAME', 'PROBABLE_NATURAL_KEY_COLUMNS']],
                                         how="left")
    # print(primary_key_df)
    resultant = primary_key_df.merge(split_by_key_df[['DATABASENAME', 'TABLENAME', 'SPLIT_BY_KEY_CANDIDATES']],
                                     how="left")
    # resultant = split_by_key_df.merge(primary_key_df[['DATABASENAME','TABLENAME','PROBABLE_NATURAL_KEY_COLUMNS']],how="left")
    resultant = resultant.assign(DERIVE_SPLIT_COLUMN_FUCTION='')
    storage_format = configuration_json.get("ingestion_storage_format", "parquet")
    resultant_df = resultant.assign(STORAGE_FORMAT=storage_format)
    resultant_df = resultant_df.assign(INGESTION_STRATEGY='')
    resultant_df = resultant_df.fillna('')
    resultant_df.reset_index(drop=True)
    print("resultant_df.info", resultant_df.info(verbose=True))

    def get_ingestion_strategy(row):
        index = row.Index
        ingest_strategy = np.where(
            len(list(map(trim_spaces, row.PROBABLE_NATURAL_KEY_COLUMNS.split(',')))) != 0 and len(list(map(trim_spaces,
                                                                                                           [value for
                                                                                                            value in
                                                                                                            configuration_json[
                                                                                                                "append_water_marks_columns"]
                                                                                                            if
                                                                                                            value in list(
                                                                                                                map(trim_spaces,
                                                                                                                    row.SPLIT_BY_KEY_CANDIDATES.split(
                                                                                                                        ',')))]))) != 0,
            'INCREMENTAL_APPEND', 'FULL_REFRESH')
        ingest_strategy = np.where(
            len(list(map(trim_spaces, row.PROBABLE_NATURAL_KEY_COLUMNS.split(',')))) != 0 and len(list(map(trim_spaces,
                                                                                                           [value for
                                                                                                            value in
                                                                                                            configuration_json[
                                                                                                                "merge_water_marks_columns"]
                                                                                                            if
                                                                                                            value in list(
                                                                                                                map(trim_spaces,
                                                                                                                    row.SPLIT_BY_KEY_CANDIDATES.split(
                                                                                                                        ',')))]))) != 0,
            'INCREMENTAL_MERGE', 'FULL_REFRESH')
        resultant_df.at[index, 'INGESTION_STRATEGY'] = ingest_strategy
        if ingest_strategy in ['INCREMENTAL_MERGE', 'INCREMENTAL_APPEND']:
            value_compare_list = [value for value in configuration_json["merge_water_marks_columns"] if
                                  value in list(map(trim_spaces, row.SPLIT_BY_KEY_CANDIDATES.split(',')))]
            watermark_column = [value for value in configuration_json["merge_water_marks_columns"] if
                                value in value_compare_list]
            watermark_column_val = watermark_column[0] if len(watermark_column) != 0 else ''
            resultant_df.at[index, 'WATERMARK_COLUMN'] = watermark_column_val

    resultant_df = resultant_df.assign(WATERMARK_COLUMN='')
    resultant_df['TARGET_SCHEMA_NAME'] = resultant_df['DATABASENAME'].apply(target_schema_name)
    resultant_df['TARGET_TABLE_NAME'] = resultant_df['TABLENAME'].apply(target_table_name)
    resultant_df = resultant_df.assign(TABLE_GROUP_NAME='')
    resultant_df = resultant_df.assign(CONNECTION_QUOTA='')
    resultant_df = resultant_df.assign(PARTITION_COLUMN='')
    resultant_df = resultant_df.assign(DERIVED_PARTITION='False')
    resultant_df = resultant_df.assign(DERIVED_FORMAT='')
    resultant_df = resultant_df.assign(SCD_TYPE_2='False')
    table_type = configuration_json.get("default_table_type", "infoworks_managed_table")
    resultant_df = resultant_df.assign(TABLE_TYPE=table_type)
    resultant_df = resultant_df.assign(USER_MANAGED_TABLE_TARGET_PATH='')
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(get_ingestion_strategy, resultant_df.itertuples())
        executor.shutdown(wait=True)
    resultant_df['TABLE_OR_VIEW'] = resultant_df['TABLE_OR_VIEW'].str.replace('BASE ', '')
    print("done with INGESTION_STRATEGY ")
    print(resultant_df.head(10))
    try:
        resultant_df.to_csv(metadata_csv_path, index=False)
        print(f"Please find the intermediate CSV file at {metadata_csv_path}")
    except Exception as e:
        print(str(e))
        exit(0)


if __name__ == '__main__':
    main()
