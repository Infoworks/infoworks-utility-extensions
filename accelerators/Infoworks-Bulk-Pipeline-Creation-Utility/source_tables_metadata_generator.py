import sys
import json
import os
from os.path import exists
import pandas as pd
import argparse
from collections import defaultdict
import urllib3
import logging
required = {'infoworkssdk==4.0a6'}
import pkg_resources
import subprocess
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.client import InfoworksClientSDK
import infoworks.sdk.local_configurations

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def configure_logger(folder, filename):
    log_level = 'DEBUG'
    log_file = "{}/{}".format(folder, filename)
    if os.path.exists(log_file):
        os.remove(log_file)

    file_console_logger = logging.getLogger('BulkTableConfigure')
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

class SourceTablesMetadata():
    def __init__(self,source_id,table_ids,exclude_table_ids,iwx_client,metadata_csv_path,config_file_json):
        self.source_id = source_id
        self.table_ids = table_ids
        self.exclude_table_ids = exclude_table_ids
        self.iwx_client = iwx_client
        self.metadata_path = metadata_csv_path
        self.configuration_json = config_file_json

    def get_source_tables_metadata(self):
        params = {"sort_by": "_id"}
        if self.table_ids:
            params["filter"]={"_id":{"$in":self.table_ids}}
        if self.exclude_table_ids:
            params["filter"]={"_id":{"$nin":self.exclude_table_ids}}

        source_tables_response = self.iwx_client.list_tables_in_source(source_id=self.source_id, params=params)
        source_tables = source_tables_response.get("result",{}).get("response",{}).get("result",[])
        if source_tables==[]:
            logger.error("Source doesn't exist or does not contain any tables. Please validate")
            logger.info(source_tables_response)
            exit(-100)
        return source_tables

    def create_pipeline_metadata_csv(self,source_tables):
        pipeline_metadata = []
        for table in source_tables:
            #print(json.dumps(table,indent=4))
            temp={}
            logger.info(f"Collecting details for Table Name : {table['name']}")
            pipeline_prefix = self.configuration_json.get("pipeline_name_prefix","pipeline")
            temp["pipeline_name"] = f"{pipeline_prefix}_{table['configuration'].get('target_table_name','')}"
            temp["batch_engine"] = self.configuration_json.get("batch_engine","spark")
            if not self.configuration_json.get("batch_engine",""):
                raise ValueError("Batch engine cannot be empty in configurations.json file.Exiting..")
                exit(-100)
            temp["domain_name"] = self.configuration_json.get("domain_name","")
            if not self.configuration_json.get("domain_name",""):
                raise ValueError("Domain name cannot be empty in configurations.json file.Exiting..")
                exit(-100)
            temp["environment_name"] = self.configuration_json.get("environment_name","")
            if not self.configuration_json.get("environment_name",""):
                raise ValueError("Environment name cannot be empty in configurations.json file.Exiting..")
                exit(-100)
            temp["storage_name"] = self.configuration_json.get("storage_name","")
            temp["compute_template_name"] = self.configuration_json.get("compute_template_name","")
            temp["ml_engine"] = "SparkML"
            temp["source_table_name"] = f"{table['name']}"
            temp["source_table_schema"] = f"{table['schema_name_at_source']}"
            temp["catalog_name"] = f"{table['catalog_name']}"
            temp["catalog_is_database"] = f"{table['catalog_is_database']}"
            natural_keys = table.get("configuration",{}).get("natural_keys",[])
            if natural_keys is None or natural_keys==[]:
                temp["natural_keys"] = ""
            else:
                temp["natural_keys"] = ",".join(natural_keys)
            temp["sync_type"] = table["configuration"]["sync_type"]
            temp["update_strategy"] = table["configuration"].get("update_strategy","")
            watermark_columns = table.get("configuration", {}).get("watermark_columns", [])
            if watermark_columns is None or watermark_columns == []:
                temp["watermark_columns"] = ""
            else:
                temp["watermark_columns"] = ",".join(watermark_columns)
            temp["target_table_name"] = table["configuration"].get("target_table_name","")
            temp["target_table_schema"] = self.configuration_json.get("target_schema_name",f"{table['configuration']['target_schema_name']}")
            temp["target_base_path"] = self.configuration_json.get("target_base_path","")
            temp["additional_params"] = ""
            temp["source_id"] = table['source']
            temp["table_id"] = table['id']
            pipeline_metadata.append(temp)
        df = pd.DataFrame(pipeline_metadata)
        logger.info(f"Writing the metadata to {self.metadata_path}")
        df.to_csv(self.metadata_path,index=False)

def main():
    cwd = os.path.dirname(__file__)
    global iwx_client, logger, configuration_json, refresh_token, metadata_csv_path
    parser = argparse.ArgumentParser(description='Bulk Pipeline creation')
    parser.add_argument('--source_id', type=str, required=True,
                        help='source_id which contains the source tables')
    parser.add_argument('--include_table_ids', nargs='+',default=[], required=False,
                        help='Pass the selective source tables to create pipeline(Default all tables)')
    parser.add_argument('--exclude_table_ids', nargs='+', default=[], required=False,
                        help='Pass the selective source tables to be excluded(Default no tables will be excluded)')
    parser.add_argument('--metadata_csv_path', type=str, required=False,default=f"{cwd}/metadata_csv/iwx_source_name_pipeline_metadata.csv",
                        help='Pass the path where pipeline_metadata is to be stored along with filename (optional argument)')
    parser.add_argument('--config_json_path', type=str, required=True,
                        help='Pass the absolute path of configuration json file')
    args = parser.parse_args()
    configuration_json={}
    metadata_csv_path=""
    log_file_path = f"{cwd}/logs"
    if not os.path.exists(log_file_path):
        os.makedirs(log_file_path)
    logs_folder = f'{cwd}/logs/'
    log_filename = 'pipeline_bulk_configuration.log'
    logger = configure_logger(logs_folder, log_filename)
    if os.path.exists(args.config_json_path):
        with open(args.config_json_path,"r") as f:
            configuration_json = json.load(f)
    else:
        logger.error(f"Specified configuration json path {args.config_json_path} doesn't exist.Please validate and rerun.Exiting..")
        exit(-100)
    source_id = args.source_id
    table_ids = args.include_table_ids
    exclude_table_ids = args.exclude_table_ids
    metadata_csv_path = args.metadata_csv_path
    infoworks.sdk.local_configurations.REQUEST_TIMEOUT_IN_SEC = 30
    infoworks.sdk.local_configurations.MAX_RETRIES = 3
    iwx_client = InfoworksClientSDK()
    iwx_client.initialize_client_with_defaults(configuration_json.get('protocol', 'http'),
                                               configuration_json.get('host', 'localhost'),
                                               configuration_json.get('port', '3001'),
                                               configuration_json.get("refresh_token",""))
    source_name = iwx_client.get_sourcename_from_id(source_id)
    source_name = source_name.get("result", {}).get("response", {}).get("name", "")
    if source_name == "":
        logger.error(f"Could not find the source with id : {source_id}.Exiting...")
        exit(-100)
    metadata_csv_path = metadata_csv_path.replace("iwx_source_name", f"{source_name}")
    source_tables_obj = SourceTablesMetadata(source_id,table_ids,exclude_table_ids,iwx_client,metadata_csv_path,configuration_json)
    source_tables_list = source_tables_obj.get_source_tables_metadata()
    source_tables_obj.create_pipeline_metadata_csv(source_tables_list)

if __name__ == '__main__':
    main()