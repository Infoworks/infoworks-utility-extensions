import argparse
import json
import logging
import os
import subprocess
import sys
import time
import uuid
import pandas as pd
import urllib3
import copy
#sys.path.insert(0,"/Users/nitin.bs/PycharmProjects/infoworks-python-sdk/")
required = {'infoworkssdk==4.0a6'}
import pkg_resources
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

class BulkPipeline():
    def __init__(self,iwx_client,metadata_csv_path,configuration_json,target_node_type,target_node_properties,sql_file_path):
        self.iwx_client = iwx_client
        self.metadata_csv_path = metadata_csv_path
        self.configuration_json = configuration_json
        self.target_node_type = target_node_type
        self.target_node_properties = target_node_properties
        self.sql_file_path = sql_file_path
        self.target_sql = ''
    def prepare_domain_name_to_id_mappings(self):
        self.domains_name_to_id_mappings={}
        list_domains_response = self.iwx_client.list_domains()
        domains = list_domains_response.get("result",{}).get("response",{}).get("result",[])
        for domain in domains:
            self.domains_name_to_id_mappings[domain["name"].lower()]=domain["id"]

    def replace_placeholders(self,sql_query, csv_columns,row):
        # Replace placeholders with actual column values
        for column in csv_columns:
            sql_query = sql_query.replace('{'+f'{column}'+'}', str(row[column]))
        # source_table_schema_response = self.iwx_client.get_table_schema(source_id=row['source_id'],table_id=row['table_id'])
        # source_table_columns = source_table_schema_response.get('result',{}).get('response',{}).get('result',[])
        # source_table_columns_names = [column['original_name'] for column in source_table_columns if not column['original_name'].lower().startswith('ziw')]
        #print("source_table_details",source_table_details)
        #sql_query = sql_query.replace('*',','.join(source_table_columns_names))
        print("sql_query",sql_query)
        return sql_query

    def prepare_environment_name_to_id_mappings(self):
        self.environments_name_to_id_mappings={}
        self.environment_types_mappings={}
        list_environment_response = self.iwx_client.get_environment_details()
        environments = list_environment_response.get("result",{}).get("response",{}).get("result",[])
        for env in environments:
            self.environments_name_to_id_mappings[env["name"].lower()]=env["id"]
            self.environment_types_mappings[env["name"].lower()]=env.get("data_warehouse_type",env.get("platform","")).lower()

    def create_pipeline_if_not_exists(self,pipeline_name,domain_name,environment_name,compute_name,storage_name):
        domain_id = self.domains_name_to_id_mappings.get(domain_name.lower(),None)
        logger.info("Checking if pipeline with same name already exists")
        pipeline_details = self.iwx_client.list_pipelines(domain_id=domain_id,params={"filter":{"name":pipeline_name}}).get("result",{}).get("response",{}).get("result",[])
        if pipeline_details:
            pipeline_id = pipeline_details[0]["id"]
            logger.info(f"Found an existing pipeline with the same name. Using its id {pipeline_id}")
            return domain_id,pipeline_id
        #print(f"environment name : {environment_name}")
        environment_id = self.environments_name_to_id_mappings.get(environment_name.lower(),None)
        #print(f"environment id : {environment_id}")
        environment_type = self.environment_types_mappings.get(environment_name.lower(),None)
        if domain_id is None:
            logger.error(f"Could not find the domain {domain_name}.Please validate.Exiting...")
            exit(-100)
        if environment_id is None:
            logger.error(f"Could not find the environment {environment_name}.Please validate.Exiting...")
            exit(-100)
        pipeline_config_body = {
            "name": pipeline_name,
            "batch_engine": "SPARK",
            "domain_id": domain_id,
            "environment_id": environment_id
            }
        if environment_type not in ["snowflake","bigquery"]:
            pipeline_config_body["batch_engine"]="SPARK"
            pipeline_config_body["ml_engine"] = "SPARK"
            compute_id = None
            storage_id = None
            if None in [compute_name,storage_name]:
                logger.error(f"Compute or Storage Name cannot be empty for Databricks environment {environment_name}")
                exit(-100)
            compute_details = self.iwx_client.get_compute_template_details(environment_id=environment_id,params={"filter":{"name":compute_name}}).get("result",{}).get("response",{}).get("result",[])
            if compute_details==[]:
                compute_details = self.iwx_client.get_compute_template_details(environment_id=environment_id,is_interactive=True, params={"filter":{"name":compute_name}}).get("result",{}).get("response",{}).get("result",[])
            if len(compute_details)>0:
                compute_id =  compute_details[0]["id"]
            else:
                logger.error(f"Could not find the compute name {compute_name} under given environment {environment_name}.Exiting..")
                exit(-100)
            storage_details = self.iwx_client.get_storage_details(environment_id=environment_id, params={
                "filter": {"name": storage_name}}).get("result", {}).get("response", {}).get("result", [])
            if len(storage_details) > 0:
                storage_id = storage_details[0]["id"]
            else:
                logger.error(
                    f"Could not find the storage name {compute_name} under given environment {environment_name}.Exiting..")
                exit(-100)
            if compute_id:
                pipeline_config_body["compute_template_id"]=compute_id
            if storage_id:
                pipeline_config_body["storage_id"] = storage_id
        #logger.info("pipeline body:{pipeline_config_body}")
        pipeline_create_response = self.iwx_client.create_pipeline(pipeline_config=pipeline_config_body)
        pipeline_id = pipeline_create_response.get("result",{}).get("response",{}).get("result",{}).get("id",None)
        if pipeline_id:
            logger.info("pipeline created successfully")
        else:
            logger.info("pipeline creation failed")
            logger.info(pipeline_create_response)
        return domain_id,pipeline_id

    def prepare_sql_for_import(self,columns,row):
        final_sql = self.replace_placeholders(self.target_sql,columns,row)
        return final_sql

    def import_sql_to_pipeline(self,domain_id,pipeline_id,sql,retries=3):
        sql_import_response = self.iwx_client.sql_import_into_pipeline(domain_id=domain_id,pipeline_id=pipeline_id,sql_query=sql)
        if sql_import_response["result"]["status"]=="success":
            sql_import_response.get("result",{}).get("response",{})
            logger.info(f"sql import success:{sql_import_response}")
            time.sleep(5)
        else:
            logger.error(f"sql import failed :{sql_import_response}.Retrying...")
            time.sleep(5)
            if retries!=0:
                retries-=1
                self.import_sql_to_pipeline(domain_id, pipeline_id, sql,retries)
            raise Exception(f"sql import failed :{sql_import_response}")

    def bulk_old_pipeline_versions_deletion(self,domain_id,pipeline_id):
        pipeline_versions_list_response = self.iwx_client.list_pipeline_versions(domain_id=domain_id, pipeline_id=pipeline_id)
        pipeline_versions_list = pipeline_versions_list_response.get("result",{}).get("response",{}).get("result",[])
        if pipeline_versions_list==[]:
            logger.error("Could not list the pipeline versions")
            logger.error(pipeline_versions_list_response)
        pipeline_versions_ids = [pipeline_version["id"] for pipeline_version in pipeline_versions_list]
        for pipeline_version_id in pipeline_versions_ids:
            if pipeline_version_id!=self.active_pipeline_version:
                logger.info(f"Active version id {self.active_pipeline_version}")
                logger.info(f"Deleting pipeline version {pipeline_version_id}")
                res = self.iwx_client.delete_pipeline_version(domain_id=domain_id, pipeline_id=pipeline_id,pipeline_version_id=pipeline_version_id)
                logger.info(res.get("result",{}).get("response",{}).get("message",""))

    def bulk_pipeline_creation_and_updation(self):
        df = pd.read_csv(self.metadata_csv_path)
        for index, row in df.iterrows():
            pipeline_name = row["pipeline_name"]
            domain_name = row["domain_name"]
            environment_name = row["environment_name"]
            compute_name = row["compute_template_name"]
            storage_name = row["storage_name"]
            source_schema_name = row["source_table_schema"]
            source_table_name = f"{row['source_table_name']}"
            target_schema_name = f"{row['target_table_schema']}"
            target_table_name = f"{row['target_table_name']}"
            additional_params = row['additional_params']
            natural_keys = row['natural_keys']
            natural_keys = natural_keys if not isinstance(natural_keys, float) else ""
            sync_type=row['sync_type']
            update_strategy=row['update_strategy']
            logger.info(f"Creating pipeline {pipeline_name} under {domain_name} domain if not exists")
            domain_id,pipeline_id = self.create_pipeline_if_not_exists(pipeline_name,domain_name,environment_name,compute_name,storage_name)
            csv_columns = list(df.columns)
            sql_for_import = self.prepare_sql_for_import(csv_columns,row)
            logger.info(sql_for_import)
            sql_import = self.import_sql_to_pipeline(domain_id=domain_id,pipeline_id=pipeline_id,sql=sql_for_import)
            pipeline_configuration_json = self.iwx_client.export_pipeline_configurations(pipeline_id=pipeline_id, domain_id=domain_id)
            pipeline_configuration_json=pipeline_configuration_json.get("result",{}).get("response",{}).get("result",{})
            if pipeline_configuration_json!={}:
                #code to change target node
                nodes = pipeline_configuration_json["configuration"]["pipeline_configs"]["model"]["nodes"]
                graph = pipeline_configuration_json["configuration"]["pipeline_configs"]["model"]["pipeline_graph"]
                target_node_name=None
                target_node_existing_properties=None
                target_node_index_in_graph=-1
                for key,node in nodes.items():
                    if node["type"]=="TARGET":
                        target_node_name=key
                        target_node_existing_properties = copy.deepcopy(node)
                    elif node["type"]=="SOURCE_TABLE":
                        node["properties"]["load_incrementally"]=self.configuration_json.get("load_data_incrementally", False)
                    else:
                        pass
                for i,target_node_property in enumerate(self.target_node_properties):
                    index = len(str(i))
                    key = key[:-index]+str(i)
                    updated_target_node_name = f"{self.target_node_type.upper().replace('_TARGET','')}_{key}"
                    updated_target_node_properties= copy.deepcopy(target_node_existing_properties)
                    updated_target_node_properties["type"]=self.target_node_type.upper()
                    updated_target_node_properties["id"] = updated_target_node_name
                    updated_target_node_properties["name"] = f"{self.target_node_type.upper()}_{target_table_name}_"+str(i)
                    output_entities=updated_target_node_properties["output_entities"].copy()
                    input_entities=updated_target_node_properties["input_entities"].copy()
                    updated_output_entities = []
                    #code to remove audit columns from target
                    if self.configuration_json.get("exclude_audit_columns",True):
                        for column in  output_entities:
                            if column["name"].lower().startswith("ziw"):
                                pass
                            else:
                                updated_output_entities.append(column)
                        updated_target_node_properties["output_entities"]=updated_output_entities
                    #code to include any excluded derive columns
                    updated_input_entities=[]
                    prev_column = {}
                    for column in input_entities:
                        if column["entity_attributes"].get("is_excluded",False):
                            column["entity_attributes"]["is_excluded"] = False
                            column["mapping"]["reference_type"]="PORT"
                            temp=copy.deepcopy(column)
                            temp["mapping"]["reference_type"]="INPUT"
                            temp["mapping"]["reference_id"]=temp["id"]
                            temp["mapping"].pop("reference_port_id")
                            temp["mapping"].pop("reference_index")
                            temp["entity_attributes"].pop("is_excluded")
                            temp["entity_attributes"]["data_type"]=""
                            column["mapping"].pop("reference_index")
                            new_uuid = uuid.uuid4()
                            uuid_str = str(new_uuid)
                            temp["id"]=uuid_str
                            updated_target_node_properties["output_entities"].append(temp)
                            updated_input_entities.append(column)
                        else:
                            updated_input_entities.append(column)
                    updated_target_node_properties["input_entities"]=updated_input_entities
                    if self.target_node_type=="custom_target":
                        for mapping in target_node_property.get("mappings",{}):
                            if mapping.get("key","")=="table_name":
                                mapping["value"]=target_table_name
                            if mapping.get("key","")=="schema_name":
                                mapping["value"]=target_schema_name
                            if mapping.get("key","")=="build_mode":
                                if sync_type=="full-load":
                                    mapping["value"] = "overwrite"
                                else:
                                    mapping["value"] = update_strategy.lower()
                            if mapping.get("key", "") == "natural_key":
                                pass
                        if natural_keys!="":
                            natural_key_map = {"key":"natural_key","value":natural_keys}
                            target_node_property["mappings"].append(natural_key_map)
                    elif self.target_node_type=="target":
                        target_node_property["target_table_name"] = target_table_name
                        target_node_property["target_schema_name"] = target_schema_name
                        target_node_property["target_base_path"] = f"{row.get('target_base_path',target_node_property.get('target_base_path',''))}"
                        if natural_keys is None or natural_keys == "":
                            target_node_property["natural_key"]=[]
                        else:
                            target_node_property["natural_key"]=[column.lower() for column in natural_keys.split(",")]
                    else:
                        if natural_keys is None or natural_keys == "":
                            target_node_property["natural_key"]=[]
                        else:
                            target_node_property["natural_key"]=[column.lower() for column in natural_keys.split(",")]
                        target_node_property["table_name"] = target_table_name
                        target_node_property["schema_name"] = target_schema_name
                        if not target_node_property.get("build_mode",""):
                            if sync_type == "full-load":
                                target_node_property["build_mode"] = "OVERWRITE"
                            else:
                                target_node_property["build_mode"] = update_strategy.upper()
                    if not isinstance(additional_params, float):
                        for param in additional_params.split("|"):
                            parts = param.split("=")
                            if len(parts) > 2 and parts[0] == 'spark_write_options':
                                key = parts[0]
                                value = '='.join(parts[1:])
                            else:
                                key = parts[0]
                                value = parts[1]
                            target_node_property["mappings"].append({"key":key,"value":value})

                    updated_target_node_properties["properties"] = target_node_property
                    nodes[updated_target_node_name]=updated_target_node_properties
                    new_links = []
                    for index,node in enumerate(graph):
                        for k,v in node.items():
                            if v==target_node_name:
                                #node[k]=updated_target_node_name
                                temp=node.copy()
                                temp[k]=updated_target_node_name
                                new_links.append(temp)
                                target_node_index_in_graph=index
                    graph.extend(new_links)

                    pipeline_configuration_json["import_configs"]={
                        "run_pipeline_metadata_build": False,
                        "is_pipeline_version_active": True,
                        "import_data_connection": True,
                        "include_optional_properties": True
                        }
                    data_connection_id = updated_target_node_properties["properties"].get("data_connection_id",None)
                    data_connection_name = updated_target_node_properties["properties"].get("data_connection_name",None)
                    if data_connection_id is not None and data_connection_name is not None and self.target_node_type not in ["custom_target"]:
                        pipeline_configuration_json["configuration"]["iw_mappings"].append({
                            "entity_type": "data_connection",
                            "entity_id": data_connection_id,
                            "recommendation": {
                                "data_connection_name": data_connection_name,
                                "data_connection_subtype": updated_target_node_properties["properties"]["data_connection_sub_type"]
                            },
                            "entity_subtype": updated_target_node_properties["properties"]["data_connection_sub_type"]
                        })
                        updated_target_node_properties["properties"].pop(data_connection_name,"")
                    else:
                        if self.target_node_type not in ["custom_target","target"]:
                            logger.error("Data connection name or Data connection ID cannot be empty in target properties json.Exiting..")
                            exit(-100)
                # remove any reference to old target
                #print("graph:",graph)
                #print("target_node_index_in_graph",target_node_index_in_graph)
                nodes.pop(target_node_name,{})
                if target_node_index_in_graph!=-1:
                    graph.pop(target_node_index_in_graph)
                #print("updated_graph:", graph)

                # Update the view options for proper orientation and spacing of nodes
                update_pipeline_view_style= {"view_options":{
                "linkStyle": "orthogonal",
                "layout": {
                    "orientation": "horizontal",
                    "layerSpacing": 25
                }
                }}
                #with open(f"./conf/temp_{source_table_name}.json","w") as f:
                #    json.dump(pipeline_configuration_json,f)
                #pipeline_configuration_json["configuration"]["entity"]["subEntityName"]='V1'
                pipeline_config_import_response = self.iwx_client.import_pipeline_configurations(pipeline_id=pipeline_id, domain_id=domain_id, pipeline_config=pipeline_configuration_json)
                #print(json.dumps(pipeline_config_import_response,indent=4))
                if pipeline_config_import_response["result"]["status"]=="success":
                    logger.info("Pipeline target properties updated successfully")
                    pipeline_version_id =pipeline_config_import_response["result"]["response"]["result"]["configuration"]["additional_data"][0]["value"]
                    #versions=self.iwx_client.list_pipeline_versions(domain_id=domain_id,pipeline_id=pipeline_id,params={"filter":{"is_active":True}})
                    try:
                        self.active_pipeline_version = pipeline_version_id
                        print(f"Active version ID:{self.active_pipeline_version}")
                        self.iwx_client.update_pipeline_version(pipeline_id=pipeline_id,domain_id=domain_id,pipeline_version_id=pipeline_version_id,body=update_pipeline_view_style)
                    except IndexError as e:
                        logger.error("No active version for pipeline found.Exiting...")
                        exit(-100)
                else:
                    logger.error("Pipeline target properties updation failed")
                    logger.error(pipeline_config_import_response)
            if self.configuration_json.get("delete_older_pipeline_versions", False):
                logger.info("Deleting any older pipeline versions other than active version")
                self.bulk_old_pipeline_versions_deletion(domain_id, pipeline_id)

def main():
    cwd = os.path.dirname(__file__)
    global iwx_client, logger, configuration_json, refresh_token, metadata_csv_path
    parser = argparse.ArgumentParser(description='Bulk Pipeline creation')
    parser.add_argument('--metadata_csv_path', type=str, required=True,
                        help='Pass the path where pipeline_metadata is stored along with filename')
    parser.add_argument('--config_json_path', type=str, required=True,
                        help='Pass the absolute path of configuration json file')
    parser.add_argument('--sql_file_path',type=str,required=True,help='Pass the absolute path of sql file that will be used for sql import')
    parser.add_argument('--target_node_type', type=str, required=True,choices=["custom_target","sql_server_target","oracle_target","target","snowflake_target"],
                        help='pipeline target node type [custom_target,sql_server_target,oracle_target,target,snowflake_target]')
    parser.add_argument('--target_node_properties_file', nargs='+'  , required=True,
                        help='Pass the absolute path of target properties configuration json file. If passing multiple files ,pass it as space separated')
    args = parser.parse_args()
    configuration_json={}
    metadata_csv_path=""
    log_file_path = f"{cwd}/logs"
    sql_file_path=args.sql_file_path
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
    target_node_type = args.target_node_type
    target_node_properties_files = args.target_node_properties_file
    target_node_properties=[]
    for file in target_node_properties_files:
        if os.path.exists(file):
            with open(file,"r") as f:
                temp_target_node_properties = json.load(f)
                target_node_properties.append(temp_target_node_properties)
        else:
            logger.error(f"Specified target_node_properties_file json path {file} doesn't exist.Please validate and rerun.Exiting..")
            exit(-100)
    metadata_csv_path = args.metadata_csv_path
    infoworks.sdk.local_configurations.REQUEST_TIMEOUT_IN_SEC = 30
    infoworks.sdk.local_configurations.MAX_RETRIES = 3
    iwx_client = InfoworksClientSDK()
    iwx_client.initialize_client_with_defaults(configuration_json.get('protocol', 'http'),
                                               configuration_json.get('host', 'localhost'),
                                               configuration_json.get('port', '3001'),
                                               configuration_json.get("refresh_token",""))
    pipeline_obj = BulkPipeline(iwx_client,metadata_csv_path,configuration_json,target_node_type,target_node_properties,sql_file_path)
    if os.path.exists(sql_file_path):
        with open(sql_file_path,"r") as f:
            pipeline_obj.target_sql = f.read()
    else:
        logger.error(f"Specified sql_file_path json path {sql_file_path} doesn't exist.Please validate and rerun.Exiting..")
        exit(-100)
    pipeline_obj.prepare_domain_name_to_id_mappings()
    pipeline_obj.prepare_environment_name_to_id_mappings()
    #print(pipeline_obj.domains_name_to_id_mappings)
    #print(pipeline_obj.environments_name_to_id_mappings)
    #print(pipeline_obj.environment_types_mappings)
    #pipeline_obj.bulk_pipeline_creation_and_updation()
    pipeline_obj.bulk_pipeline_creation_and_updation()
    #print(iwx_client.get_pipeline(domain_id="651275bdbae4f565acf7f9f4",pipeline_id="6513c0fc8845df20117006c5"))
if __name__ == '__main__':
    main()