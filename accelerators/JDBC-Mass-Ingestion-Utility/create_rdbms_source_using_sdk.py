# python create_rdbms_source.py --source_name <name of the source to create>  --source_type <teradata/oracle/netezza> --refresh_token <your_refresh_token> --metadata_csv_file <path to metadata csv file including filename>
import argparse
import json
import os
import sys
import traceback
import pandas as pd
import urllib3
import logging
import pkg_resources
import subprocess
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
required = {'infoworkssdk==3.0b0'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    python = sys.executable
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
from infoworks.sdk.client import InfoworksClientSDK

class CustomError(Exception):
    def __init__(self, message):
        self.message = message
        super(CustomError, self).__init__(self.message)
cwd = os.path.dirname(os.path.realpath(__file__))
configuration_json={}
proxy_host=''
proxy_port=''
protocol = ''

driver_name_mappings={
    "netezza":"org.netezza.Driver",
    "teradata":"com.teradata.jdbc.TeraDriver",
    "oracle":"oracle.jdbc.driver.OracleDriver",
    "mysql":"org.mariadb.jdbc.Driver"
}

connection_url_mappings={
    "netezza":"jdbc:netezza://<ip>:<port>/<db>",
    "teradata":"jdbc:teradata://<ip>/DBS_PORT=<port>,DATABASE=<db>",
    "oracle":"jdbc:oracle:thin:@<ip>:<port>:xe",
    "mysql":"jdbc:mysql://<ip>:<port>/<db>"
}

def configure_logger(folder, filename):
    log_level = 'DEBUG'
    log_file = "{}/{}".format(folder, filename)
    if os.path.exists(log_file):
        os.remove(log_file)
    file_console_logger = logging.getLogger('Automated RDBMS source creation')
    file_console_logger.setLevel(log_level)
    formatter = logging.Formatter(
        "[%(asctime)s - %(levelname)s - %(filename)25s:%(lineno)s - %(funcName)20s() ] %(message)s")
    log_handler = logging.FileHandler(filename=log_file)
    log_handler.setLevel(log_level)
    log_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s — [ %(levelname)s ] - %(message)s')
    console_handler.setFormatter(formatter)
    file_console_logger.addHandler(log_handler)
    file_console_logger.addHandler(console_handler)
    print("Check the log file {} for detailed logs".format(log_file))
    return file_console_logger

def extract_result_from_response(response,default_result):
    return response.get("result", {}).get("response", {}).get("result", default_result)

def trim_spaces(item):
    return item.strip()

def get_environment_type_from_name(iwx_client,environment_id):
    try:
        env_details_response = iwx_client.get_environment_details(environment_id=environment_id)
        env_details = extract_result_from_response(env_details_response,default_result={})
        if env_details == {}:
            logger.error(f"Failed to find environment type corresponding to name {environment_id}")
            logger.info(env_details_response)
            sys.exit(-100)
        else:
            result = env_details[0]
            if len(result)!=0:
                return result.get("data_warehouse_type","") if result.get("data_warehouse_type","")!="" else  result["platform"]
            else:
                logger.error(f"Failed to find environment type corresponding to name {environment_id}")
                sys.exit(-100)
    except Exception as e:
        traceback.print_exc()
        logger.error(e, exc_info=True)
        sys.exit(-100)

def prepare_source_creation_body(iwx_client,source_name,source_type,environment_id,storage_id):
    source_body={}
    if get_environment_type_from_name(iwx_client,environment_id) == "snowflake":
        source_body={
            "name": source_name,
            "type": "rdbms",
            "sub_type": source_type,
            "data_lake_path": f"/iw/sources/{source_name}",
            "data_lake_schema": configuration_json.get("sfSchema","PUBLIC"),
            "environment_id": environment_id,
            "storage_id": storage_id,
            "quoted_identifier": "\"",
            "target_database_name": configuration_json.get("sfDatabase",source_name.upper()),
            "is_database_case_sensitive": False,
            "is_schema_case_sensitive": False,
            "staging_schema_name": configuration_json.get("sfStageSchema","PUBLIC"),
            "is_staging_schema_case_sensitive": False,
            "transformation_extensions": []
        }
    else:
        source_body = {
            "name": source_name,
            "type": "rdbms",
            "sub_type": source_type,
            "data_lake_path": f"/iw/sources/{source_name}",
            "data_lake_schema": configuration_json.get("TargetSchema", "PUBLIC"),
            "environment_id": environment_id,
            "storage_id": storage_id,
            "transformation_extensions": []
        }

    return source_body

def prepare_source_connection_details(iwx_client,source_type,environment_id):
    default_source_db=configuration_json.get("source_connection_properties",{}).get("default_source_db","")
    host=configuration_json.get("source_connection_properties",{}).get("host","")
    port=configuration_json.get("source_connection_properties",{}).get("port","")
    connection_body={
    "driver_name": driver_name_mappings.get(source_type,""),
    "connection_url": connection_url_mappings[source_type].replace("<ip>",host).replace("<port>",str(port)).replace("<db>",default_source_db),
    "username": configuration_json.get("source_connection_properties",{}).get("username",""),
    "password": configuration_json.get("source_connection_properties",{}).get("encrypted_password",""),
    "connection_mode": "jdbc",
    "database": source_type.upper()
    }
    default_warehouse_response = iwx_client.get_environment_default_warehouse(environment_id)
    default_warehouse = default_warehouse_response.get("result",{}).get("response",{}).get("default_warehouse","")
    if configuration_json.get("source_connection_properties",{}).get("connection_url","")!="":
        connection_body["connection_url"]=configuration_json.get("source_connection_properties",{}).get("connection_url","")
    if get_environment_type_from_name(iwx_client,environment_id)=="snowflake":
        connection_body["warehouse"]=configuration_json.get("source_connection_properties",{}).get("sf_warehouse","") if configuration_json.get("source_connection_properties",{}).get("sf_warehouse","") else default_warehouse
    return connection_body

def find_already_added_tables(iwx_client,source_id):
    tables_list=[]
    try:
        already_added_tables = []
        tables_list_response = iwx_client.list_tables_in_source(source_id=source_id)
        tables_list=extract_result_from_response(tables_list_response,default_result=[])
        if tables_list!=[]:
            for table in tables_list:
                print(table)
                already_added_tables.append((table.get('original_table_name',''),table.get('schema_name_at_source','')))
            return already_added_tables
        else:
            return []
    except Exception as e:
        traceback.print_exc()
        logger.error(e, exc_info=True)
        sys.exit(-100)

def main():
    global refresh_token,delegation_token,logger,configuration_json,proxy_port,proxy_host,protocol
    parser = argparse.ArgumentParser('RDBMS source create')
    parser.add_argument('--source_name', required=True, help='Pass the name of RDBMS source to be created')
    parser.add_argument('--source_type', required=True, help='Pass the name of RDBMS source type',choices=['teradata','oracle','netezza'])
    parser.add_argument('--refresh_token',type=str,required=True,help='Pass the refresh token of user with admin privileges')
    parser.add_argument('--metadata_csv_file',type=str,required=True,help='Pass the metadata csv absolute path')
    parser.add_argument('--configuration_json_file', type=str, required=True, help='Pass the configuration json file absolute path')
    #parser.add_argument('--environment_name', required=False,default="", help='Pass the name of the environment that source should use')
    #parser.add_argument('--environment_storage_name', required=False,default="", help='Pass the name of the environment storage that source should use')
    #parser.add_argument('--environment_compute_name', required=False,default="", help='Pass the name of the environment compute that source should use')
    #parser.add_argument('--meta_crawl_records_count', required=False,default='', help='Pass the metadata data crawl records count to sample and extract schema')
    if not os.path.exists(f"{cwd}/logs/"):
        os.makedirs(f"{cwd}/logs/")
    logs_folder = f'{cwd}/logs/'
    log_filename = 'source_creation.log'
    logger = configure_logger(logs_folder, log_filename)
    args = vars(parser.parse_args())
    refresh_token=args.get("refresh_token")
    source_name=args.get("source_name")
    source_type=args.get("source_type")
    with open(args.get("configuration_json_file"), "r") as configuration_file:
        configuration_json = json.load(configuration_file)
    protocol=configuration_json.get("protocol","https")
    proxy_host = configuration_json.get("host", "localhost")
    proxy_port = configuration_json.get("port", "443")
    environment_name=configuration_json.get("environment_name","")
    environment_storage_name=configuration_json.get("environment_storage_name","")
    environment_compute_name=configuration_json.get("environment_compute_name","")
    metadata_csv_file=args.get("metadata_csv_file")
    table_metadata_df=pd.read_csv(metadata_csv_file)
    default_source_db = configuration_json.get("source_connection_properties", {}).get("default_source_db", "")
    logger.info(f"environment_name : {environment_name}")
    logger.info(f"environment_compute_name : {environment_compute_name}")
    logger.info(f"environment_storage_name : {environment_storage_name}")
    iwx_client = InfoworksClientSDK()
    iwx_client.initialize_client_with_defaults(protocol, proxy_host, proxy_port, refresh_token)
    environment_id_response = iwx_client.get_environment_id_from_name(environment_name=environment_name)
    environment_id_response = environment_id_response.get("result",{}).get("response",{})
    environment_id = environment_id_response.get("environment_id",None)
    environment_storage_id_response = iwx_client.get_storage_id_from_name(storage_name=environment_storage_name,environment_id=environment_id)
    environment_storage_id_response = environment_storage_id_response.get("result",{}).get("response",{})
    environment_storage_id = environment_storage_id_response.get("storage_id",None)
    environment_compute_id_response = iwx_client.get_compute_id_from_name(compute_name=environment_compute_name, environment_id=environment_id)
    environment_compute_id_response = environment_compute_id_response.get("result",{}).get("response",{})
    environment_compute_id = environment_compute_id_response.get("compute_id",None)
    logger.info(f"environment_id:{environment_id}")
    logger.info(f"environment_compute_id:{environment_compute_id}")
    if None in [environment_id,environment_storage_id,environment_compute_id]:
        logger.error("environment_id or environment_storage_id or environment_compute_id cannot be None.\n Please validate the names passed in configurations.json.Exiting..")
        exit(-100)
    #prepare the source creation body
    source_creation_body=prepare_source_creation_body(iwx_client,source_name,source_type,environment_id,environment_storage_id)
    logger.info(f"source creation body: {source_creation_body}")
    logger.info("Checking if source with same name already exists..")
    source_exists_response = iwx_client.get_list_of_sources(params={"filter":{"name":source_creation_body["name"]}})
    source_exists_results =  extract_result_from_response(source_exists_response,default_result=[])
    source_id=""
    source_creation_response={}
    if source_exists_results != []:
        source_details = source_exists_results[0]
        source_id = source_details.get("id","")
    if source_id=="":
        print("Did not find existing source with the same name.Creating new one..")
        logger.info("Did not find existing source with the same name.Creating new one..")
        source_creation_response = iwx_client.create_source(source_creation_body)
        source_creation_results = extract_result_from_response(source_creation_response,default_result={})
        source_id = source_creation_results.get("id","")
    logger.info(f"source id : {source_id}")
    if source_id=="":
        logger.error("Failed to create source.Exiting...")
        logger.error(source_creation_response)
        exit(-100)
    #prepare the source connection body
    connection_body=prepare_source_connection_details(iwx_client,source_type,environment_id)
    logger.info(f"connection_body : {connection_body}")
    iwx_client.configure_source_connection(source_id=source_id,connection_object=connection_body)
    # test_connection_response = iwx_client.source_test_connection_job_poll(source_id=source_id)
    # if extract_result_from_response(test_connection_response,default_result={}).get("status","").upper()=="COMPLETED":
    #     logger.info("Test connection successful")
    # else:
    #     logger.error("Failed to test connection to the source.Exiting..")
    #     logger.error(test_connection_response)
    #     job_result = extract_result_from_response(test_connection_response,default_result={})
    #     job_id = job_result.get("id",None)
    #     job_url = f"{protocol}://{proxy_host}:{proxy_port}/job/logs?jobId={job_id}"
    #     logger.info(f"Check the logs here: {job_url}")
    #     exit(-100)
    filter_properties=configuration_json.get('filter_properties',{})
    for filter_property in ["is_filter_enabled","catalogs_filter","tables_filter","schemas_filter"]:
        if filter_properties.get(filter_property,"")=="":
            filter_properties[filter_property]=""
    browse_source_response = iwx_client.browse_source_tables(source_id=source_id,filter_tables_properties=filter_properties,poll=True)
    if browse_source_response.get("result",{}).get("status","").upper()=="SUCCESS":
        logger.info("Browsed the tables successfully")
    else:
        logger.info("Failed to browse the source")
        logger.info(browse_source_response)
        logger.info("Failed to browse the source")
        logger.info(browse_source_response)
    already_added_tables=find_already_added_tables(iwx_client,source_id)
    logger.info(f"already_added_tables:{already_added_tables}")
    tables_to_add=[]
    for index,row in table_metadata_df.iterrows():
        table_details={}
        table_details["table_name"] = row["TABLENAME"]
        table_details["schema_name"] = row["DATABASENAME"]
        table_details["table_type"] = "TABLE"
        table_details["target_table_name"] = row["TARGET_TABLE_NAME"]
        table_details["target_schema_name"] = row["TARGET_SCHEMA_NAME"]
        if get_environment_type_from_name(iwx_client,environment_id)=="snowflake":
            table_details["target_database_name"] = configuration_json.get("sfDatabase","DEFAULT")
        if (row["TABLENAME"],row["DATABASENAME"]) not in already_added_tables:
            tables_to_add.append(table_details)
        else:
            logger.info(f"Table {row['TABLENAME']} has been already added.Skipping addition...")
            continue
    logger.info(f"tables_to_add:{tables_to_add}")
    if tables_to_add!=[]:
        tables_add_response = iwx_client.add_tables_to_source(source_id=source_id,tables_to_add_config=tables_to_add)
        if tables_add_response.get("status","").upper() == "SUCCESS":
            logger.info("Tables added successfully")
            logger.info(tables_add_response.get("result", {}).get("response", {}).get("message",""))
        else:
            logger.error("Tables add failed")
            logger.error(tables_add_response)
    logger.info("Done!")

if __name__ == '__main__':
    main()