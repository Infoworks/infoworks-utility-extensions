import configparser
import os
import sys
import argparse
import json
import pandas as pd
pd.set_option('display.max_colwidth', None)
json_path="./configurations/pipeline/pipeline.json"
pipeline_config={}
domain_id=""
from infoworks.sdk.client import InfoworksClientSDK
import urllib3
urllib3.disable_warnings()
def main():
    pipelines = []
    parser = argparse.ArgumentParser(description='Convert SQL to JSON files')
    parser.add_argument('--config_json', required=True, type=str,
                        help='Pass the config json file path along with filename')
    parser.add_argument('--config_ini_file', required=True, type=str, help='Pass the config ini file path along with filename')
    args = parser.parse_args()
    config_ini_file = args.config_ini_file
    config_json_file = args.config_json
    if not os.path.exists(config_ini_file):
        print(f"Please provide valid config_ini_file parameter.{config_ini_file} doesn't exist.Exiting...")
        exit(-100)
    if not os.path.exists(config_json_file):
        print(f"Please provide valid config_json parameter.{config_json_file} doesn't exist.Exiting...")
        exit(-100)
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(config_ini_file)
    protocol = config.get("api_mappings", "protocol")
    ip = config.get("api_mappings", "ip")
    port = config.get("api_mappings", "port")
    try:
        refresh_token = config.get("api_mappings", "refresh_token")
        if refresh_token == "":
            print("Refresh token is empty. Trying to pick it up from IWX_REFRESH_TOKEN environment variable")
            if os.getenv("IWX_REFRESH_TOKEN") is None:
                print("Could not find the refresh token in IWX_REFRESH_TOKEN env variable as well.Exiting")
                exit(-100)
            else:
                print("Found IWX_REFRESH_TOKEN env variable.Using refresh token from IWX_REFRESH_TOKEN env variable")
                refresh_token = os.getenv("IWX_REFRESH_TOKEN")
    except configparser.NoOptionError:
        print(
            "Refresh token is not available in config.ini.Trying to pick it up from IWX_REFRESH_TOKEN environment variable")
        if os.getenv("IWX_REFRESH_TOKEN") is None:
            print("Could not find the refresh token in IWX_REFRESH_TOKEN env variable as well.Exiting")
            exit(-100)
        else:
            print("Found IWX_REFRESH_TOKEN env variable.Using refresh token from IWX_REFRESH_TOKEN env variable")
            refresh_token = os.getenv("IWX_REFRESH_TOKEN")
            print(refresh_token)
    iwx_client_prd = InfoworksClientSDK()
    iwx_client_prd.initialize_client_with_defaults(protocol, ip, port, refresh_token)
    config_json={}
    with open(config_json_file,"r") as f:
        config_json = json.load(f)
    with open(f"""{config_json.get("input_directory")}/modified_files/pipeline.csv""","r") as f:
        pipelines = f.readlines()
    for pipeline in pipelines:
        domain_name = pipeline.split("#")[0]
        pipeline_name = pipeline.split("#")[1].split(".")[0]
        pipeline_details = iwx_client_prd.get_pipeline_id(domain_id=domain_id,pipeline_name=pipeline_name)
        pipeline_res = pipeline_details.get("result",[])
        print(pipeline_res)
        if pipeline_res:
            pipeline_id = pipeline_res.get("pipeline_id", "")
            if pipeline_id=="":
                print(f"pipeline not found with name {pipeline_name} under domain {domain_name}")
                exit(-100)
        print(pipeline_id)
        res = iwx_client_prd.delete_pipeline(domain_id=domain_id,pipeline_id=pipeline_id)
        print("delete response:",res)

if __name__ == '__main__':
    main()