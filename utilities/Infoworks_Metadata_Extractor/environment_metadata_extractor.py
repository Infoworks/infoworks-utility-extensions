import json
import os
import subprocess
import sys
import traceback
import pkg_resources
import logging
import argparse
import warnings
import pandas as pd
import inspect
import copy
from typing import List
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s — [ %(levelname)s ] — %(message)s",
                    datefmt='%d-%b-%y %H:%M:%S',
                    )
class Environment_Metadata_Extractor():
    def __init__(self,iwx_client,output_directory):
        self.environment_ids=[]
        self.environment_list = []
        self.environment_compute_list=[]
        self.environment_storage_list=[]
        self.output_directory = output_directory
        self.iwx_client = iwx_client
    def extract_environment_details(self):
        response = self.iwx_client.get_environment_details()
        result = response["result"]["response"]["result"]
        # print(json.dumps(result,indent=4))
        environment_ids = [env["id"] for env in result]
        self.environment_ids=environment_ids
        for environment in result:
            temp = {}
            keys = [
                "name",
                "id",
                "compute_engine",
                "connection_configuration.region",
                "connection_configuration.workspace_url",
                "connection_configuration.token.secret_id",
                "data_warehouse_type",
                "data_warehouse_configuration.connection_url",
                "data_warehouse_configuration.account_name",
                "data_warehouse_configuration.authentication_mechanism",
                "data_warehouse_configuration.snowflake_profiles",
                "platform"
            ]
            for key in keys:
                k, v = self.get_value(environment, key)
                if k == "id":
                    k = "environment_name"
                    env_details = self.iwx_client.get_environment_details(environment_id=v)
                    env_details = env_details["result"]["response"]["result"][0]
                    temp[k] = env_details["name"]
                elif k == "secret_id":
                    k = "secret_name"
                    try:
                        secret_details = self.iwx_client.get_secret_details(secret_id=v)
                        secret_details = secret_details["result"]["response"]["result"]
                        # print("secret_details",secret_details)
                        temp[k] = secret_details[0]["name"]
                    except (KeyError, IndexError) as e:
                        temp[k] = ""
                elif v =="":
                    pass
                elif k == "snowflake_profiles":
                    snowflake_profiles_updated=[]
                    for snowflake_profile in v:
                        profile_temp=copy.deepcopy(snowflake_profile)
                        secret_id = profile_temp.get("authentication_properties",{}).get("password",{}).get("secret_id","")
                        if secret_id !="":
                            secret_details = self.iwx_client.get_secret_details(secret_id=secret_id)
                            secret_details = secret_details["result"]["response"]["result"]
                            secret_name = secret_details["name"]
                            profile_temp["authentication_properties"]["password"]["secret_name"] = secret_name
                        snowflake_profiles_updated.append(profile_temp)
                    temp[k] = json.dumps(snowflake_profiles_updated)
                else:
                    temp[k] = v
            self.environment_list.append(temp)
        df = pd.DataFrame(self.environment_list)
        print(df)
        self.dataframe_writer(dataframe=df, report_type=inspect.stack()[0][3])

    def get_value(self,json_dict,nested_path):
        try:
            nested_items = nested_path.split(".")
            return_value=json_dict.copy()
            for item in nested_items:
                if item.isdigit():
                    item=int(item)
                return_value = return_value[item]
            return item,return_value
        except (KeyError,IndexError) as e:
            return item,""

    def dataframe_writer(self,dataframe,report_type):
        try:
            file_path = self.output_directory+report_type+".csv"
            print(f"Writing the data frame to file {file_path}")
            dataframe.to_csv(file_path,index=False)
            print("Report generated Successfully!")
        except Exception as e:
            print(str(e))
            raise Exception("Error generating report")

    def extract_compute_details(self):
        for environment_id in self.environment_ids:
            all_computes=[]
            response = self.iwx_client.get_compute_template_details(environment_id=environment_id)
            result = response["result"]["response"]["result"]
            all_computes.extend(result)
            # print(json.dumps(result,indent=4))
            response = self.iwx_client.get_compute_template_details(environment_id=environment_id,is_interactive=True)
            result = response["result"]["response"]["result"]
            # print(json.dumps(result, indent=4))
            all_computes.extend(result)
            keys = [
                    "name",
                    "environment_id",
                    "launch_strategy",
                    "cluster_id",
                    "compute_configuration.connection_configuration.workspace_url",
                    "compute_configuration.connection_configuration.region",
                    "compute_configuration.connection_configuration.token.service_auth_id",
                    "compute_configuration.runtime_configuration.driver_node_type",
                    "compute_configuration.runtime_configuration.worker_node_type",
                    "compute_configuration.runtime_configuration.num_worker_nodes",
                    "compute_configuration.runtime_configuration.max_allowed_workers",
                    "compute_configuration.runtime_configuration.idle_termination_timeout",
                    "compute_configuration.runtime_configuration.allow_zero_workers",
                    "compute_configuration.runtime_configuration.enable_autoscale",
                    "compute_configuration.runtime_configuration.advanced_configurations"
                    ]
            for compute in all_computes:
                temp = {}
                for key in keys:
                    k,v = self.get_value(compute, key)
                    if k=="environment_id":
                        k="environment_name"
                        env_details = self.iwx_client.get_environment_details(environment_id=environment_id)
                        env_details=env_details["result"]["response"]["result"][0]
                        #print("env_details:",env_details["name"])
                        temp[k] = env_details["name"]
                    elif k == "secret_id":
                        k="secret_name"
                        try:
                            secret_details = self.iwx_client.get_secret_details(secret_id=v)
                            secret_details = secret_details["result"]["response"]["result"]
                            #print("secret_details",secret_details)
                            temp[k] = secret_details[0]["name"]
                        except (KeyError,IndexError) as e:
                            temp[k]=""
                    elif k == "service_auth_id":
                        k="service_auth_name"
                        try:
                            service_auth_details = iwx_client.get_service_authentication_details(service_auth_id=v)
                            service_auth_details = service_auth_details["result"]["response"]["result"]
                            #print("secret_details",secret_details)
                            temp[k] = service_auth_details[0]["name"]
                        except (KeyError,IndexError) as e:
                            temp[k]=""
                    else:
                        temp[k] = v
                self.environment_compute_list.append(temp)

        df = pd.DataFrame(self.environment_compute_list)
        print(df)
        self.dataframe_writer(dataframe=df, report_type=inspect.stack()[0][3])
            # temp["workspace_url"]=result.get("compute_configuration",{}).get("connection_configuration",{}).get("workspace_url","")
            # temp["region"]=result.get("compute_configuration",{}).get("connection_configuration",{}).get("region","")
            # temp["worker_node_type"]= result.get("compute_configuration",{}).get("runtime_configuration",{}).get("worker_node_type","")
            # temp["driver_node_type"]= result.get("compute_configuration",{}).get("runtime_configuration",{}).get("driver_node_type","")
            #print(json.dumps(result, indent=4))
        pass

    def extract_storage_details(self):
        for environment_id in self.environment_ids:
            response = self.iwx_client.get_storage_details(environment_id=environment_id)
            result = response["result"]["response"]["result"]
            #print(json.dumps(result, indent=4))
            for storage in result:
                temp = {}
                keys=[
                "name",
                "environment_id",
                "storage_type",
                "storage_authentication.scheme",
                "storage_authentication.storage_account_name",
                "storage_authentication.container_name",
                "storage_authentication.account_key.secret_id",
                "is_default_storage"
                ]
                for key in keys:
                    k, v = self.get_value(storage, key)
                    if k == "environment_id":
                        k = "environment_name"
                        env_details = self.iwx_client.get_environment_details(environment_id=v)
                        env_details = env_details["result"]["response"]["result"][0]
                        temp[k] = env_details["name"]
                    elif k == "secret_id":
                        k="secret_name"
                        try:
                            secret_details = self.iwx_client.get_secret_details(secret_id=v)
                            secret_details = secret_details["result"]["response"]["result"]
                            #print("secret_details",secret_details)
                            temp[k] = secret_details[0]["name"]
                        except (KeyError,IndexError) as e:
                            temp[k]=""
                    elif v =="":
                        pass
                    else:
                        temp[k] = v
                self.environment_storage_list.append(temp)
        # print("self.environment_storage_list:",self.environment_storage_list)
        df = pd.DataFrame(self.environment_storage_list)
        print(df)
        self.dataframe_writer(dataframe=df, report_type=inspect.stack()[0][3])

def get_all_report_methods() -> List[str]:
    method_list = [func for func in dir(Environment_Metadata_Extractor) if callable(getattr(Environment_Metadata_Extractor, func)) and not func.startswith("__") and func.startswith("extract")]
    return method_list

if __name__ == "__main__":
    required = {'pandas', 'infoworkssdk==4.0'}
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed
    file_path = os.path.dirname(os.path.realpath(__file__))
    if missing:
        logging.info("Found Missing Libraries, Installing Required")
        python = sys.executable
        subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
    import warnings

    warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
    from infoworks.sdk.client import InfoworksClientSDK
    import infoworks.sdk.local_configurations

    try:
        parser = argparse.ArgumentParser(description='Extracts metadata of pipelines in Infoworks')
        parser.add_argument('--config_file', required=True, help='Fully qualified path of the configuration file')
        parser.add_argument('--reports', nargs='+',
                            help='List of reports to generate. If not specified, all will be generated.',
                            default=get_all_report_methods(), choices=get_all_report_methods())
        parser.add_argument('--output_directory', required=False, type=str, default=f"{file_path}/csv/",
                            help='Pass the directory where the reports are to be exported')
        args = parser.parse_args()
        config_file_path = args.config_file
        if not os.path.exists(config_file_path):
            raise Exception(f"{config_file_path} not found")
        with open(config_file_path) as f:
            config = json.load(f)
        if not os.path.exists(args.output_directory):
            os.makedirs(args.output_directory)
        output_directory = args.output_directory
        if not output_directory.endswith('/'):
            output_directory = output_directory + '/'
        # Infoworks Client SDK Initialization
        infoworks.sdk.local_configurations.REQUEST_TIMEOUT_IN_SEC = 60
        infoworks.sdk.local_configurations.MAX_RETRIES = 3  # Retry configuration, in case of api failure.
        iwx_client = InfoworksClientSDK()
        iwx_client.initialize_client_with_defaults(config.get("protocol", "https"), config.get("host", None),
                                                   config.get("port", 443), config.get("refresh_token", None))
        environment_metadata_extractor_obj = Environment_Metadata_Extractor(iwx_client,output_directory)
        report_methods = args.reports
        report_methods.sort(key=lambda x: getattr(Environment_Metadata_Extractor, x).__code__.co_firstlineno)
        for report in report_methods:
            if hasattr(environment_metadata_extractor_obj, report):
                method = getattr(environment_metadata_extractor_obj, report)
                method()
    except Exception as error:
        traceback.print_exc()
        logging.error("Failed to fetch environment metadata \nError: {error}".format(error=repr(error)))