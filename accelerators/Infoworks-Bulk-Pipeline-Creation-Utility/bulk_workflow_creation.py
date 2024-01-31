import sys
import json
import os
import argparse
import urllib3
import logging
import subprocess
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

class BulkWorkflow():
    def __init__(self, source_id, iwx_client, config_file_json):
        self.source_id = source_id
        self.iwx_client = iwx_client
        self.configuration_json = config_file_json
        self.domain_id = None
    def prepare_domain_name_to_id_mappings(self):
        self.domains_name_to_id_mappings={}
        list_domains_response = self.iwx_client.list_domains()
        domains = list_domains_response.get("result",{}).get("response",{}).get("result",[])
        for domain in domains:
            self.domains_name_to_id_mappings[domain["name"].lower()]=domain["id"]

    def prepare_tables_id_to_name_mappings(self):
        self.list_tables_id_to_name_mappings={}
        list_tables_response = self.iwx_client.list_tables_in_source(source_id=self.source_id)
        tables = list_tables_response.get("result",{}).get("response",{}).get("result",[])
        for table in tables:
            self.list_tables_id_to_name_mappings[table["id"]]=table["configuration"]["target_table_name"]

    def prepare_pipeline_name_to_id_mappings(self,domain_id):
        self.pipeline_name_to_id_mappings={}
        self.domain_id=domain_id
        list_pipelines_response = self.iwx_client.list_pipelines(domain_id=domain_id)
        pipelines = list_pipelines_response.get("result",{}).get("response",{}).get("result",[])
        for pipeline in pipelines:
            try:
                self.pipeline_name_to_id_mappings[pipeline["name"].lower()]=(pipeline["id"],pipeline["active_version_id"],pipeline["versions_count"])
            except KeyError as e:
                logging.error(f"Could not find the key : {str(e)}")

    def prepare_workflow_model(self,domain_id,tg_id,tables):
        base_model={
            "tasks": [
                {
                    "is_group": False,
                    "task_type": "ingest_table_group",
                    "task_id": "SI_0019",
                    "location": "0 175",
                    "title": "Ingest Source",
                    "description": "",
                    "task_properties": {
                        "source_id": self.source_id,
                        "table_group_id": tg_id,
                        "ingest_type": "all"
                    },
                    "run_properties": {
                        "trigger_rule": "all_success",
                        "num_retries": 0
                    }
                }
            ],
            "edges": [
            ]
        }
        for index,table in enumerate(tables):
            table_name = self.list_tables_id_to_name_mappings[table["table_id"]]
            pipeline_name = self.configuration_json.get("pipeline_name_prefix","")+"_"+table_name
            pipeline_id,pipeline_active_version_id,version = self.pipeline_name_to_id_mappings[pipeline_name.lower()]
            pipeline_node_id = "PB_" + str(4000 + index)
            longitude = str(index*125)
            tasks_temp={
                    "is_group": False,
                    "task_type": "pipeline_build",
                    "task_id": pipeline_node_id,
                    "location": f"135.1999969482422 {longitude}",
                    "title": f"Pipeline {table_name}",
                    "description": "",
                    "task_properties": {
                        "task_subtype": "pipeline",
                        "pipeline_id": pipeline_id,
                        "version_id": pipeline_active_version_id,
                        "version": int(version),
                        "pipeline_parameters": []
                    },
                    "run_properties": {
                        "trigger_rule": "all_success",
                        "num_retries": 0
                    }
                }
            edge_temp={
                    "from_task": "SI_0019",
                    "to_task": pipeline_node_id,
                    "category": "LINK",
                    "task_id": -index-1
                }
            base_model["tasks"].append(tasks_temp)
            base_model["edges"].append(edge_temp)

        return base_model



    def create_workflow_if_not_exists(self,domain_id,workflow_name,workflow_body):
        try:
            workflow_details_response = self.iwx_client.get_list_of_workflows(domain_id,params={"filter":{"name":workflow_name}})
            workflow_details=workflow_details_response.get("result",{}).get("response",{}).get("result",[])
            #print("workflow_details",workflow_details)
            if workflow_details==[]:
                logger.info("There is no existing workflow with same name. Creating new one")
            else:
                workflow_detail=workflow_details[0]
                logger.info("Found an existing workflow with same name, using its id instead")
                workflow_body.pop("name", "")
                workflow_body["view_options"] = {
                    "linkStyle": "normal",
                    "layout": {
                        "orientation": "horizontal",
                        "layerSpacing": 25
                    }
                }
                response = self.iwx_client.update_workflow(workflow_id=workflow_detail["id"],domain_id=domain_id, workflow_config=workflow_body)
                logger.info(response)
                return workflow_detail["id"]
            response = self.iwx_client.create_workflow(domain_id=domain_id,workflow_config=workflow_body)
            logger.info(response)

        except Exception as e:
            logger.error("Failed to create workflow")
            logger.error(str(e))
            #logger.error(response)
        result = response.get("result",{}).get("response",{}).get("result",{})
        workflow_id=result.get("id",None)
        workflow_body.pop("name", "")
        response = self.iwx_client.update_workflow(workflow_id=workflow_id, domain_id=domain_id,
                                                   workflow_config=workflow_body)
        logger.info(response)
        return workflow_id

    def bulk_workflow_creation(self):
        list_table_groups_repsonse = self.iwx_client.get_table_group_details(source_id=self.source_id)
        table_groups=list_table_groups_repsonse.get("result",{}).get("response",{}).get("result",[])
        self.prepare_tables_id_to_name_mappings()
        for table_group in table_groups:
            #print("table_group_name",table_group["name"])
            tg_details_response = self.iwx_client.get_table_group_details(source_id=self.source_id,tg_id = table_group["id"])
            tg_details = tg_details_response.get("result",{}).get("response",{}).get("result",[])
            if tg_details:
                tg_details=tg_details[0]
                tables = tg_details["tables"]
                workflow_name_prefix = self.configuration_json.get("workflow_name_prefix","WF")
                workflow_body={"name": f"{workflow_name_prefix}_"+tg_details["name"],"description": ""}
                workflow_body["workflow_graph"]=self.prepare_workflow_model(self.domain_id,tg_details["id"],tables)
                #print(json.dumps(workflow_body,indent=4))
                res=self.create_workflow_if_not_exists(domain_id=self.domain_id,workflow_name=f"{workflow_name_prefix}_"+tg_details["name"],workflow_body=workflow_body)
                logger.info(res)

def main():
    cwd = os.path.dirname(__file__)
    global iwx_client, logger, configuration_json, refresh_token
    parser = argparse.ArgumentParser(description='Bulk Workflow creation')
    parser.add_argument('--source_id', type=str, required=True,
                        help='source_id which contains the source tables')
    parser.add_argument('--config_json_path', type=str, required=True,
                        help='Pass the absolute path of configuration json file')
    args = parser.parse_args()
    configuration_json = {}
    metadata_csv_path = ""
    log_file_path = f"{cwd}/logs"
    if not os.path.exists(log_file_path):
        os.makedirs(log_file_path)
    logs_folder = f'{cwd}/logs/'
    log_filename = 'workflow_bulk_configuration.log'
    logger = configure_logger(logs_folder, log_filename)
    if os.path.exists(args.config_json_path):
        with open(args.config_json_path, "r") as f:
            configuration_json = json.load(f)
    else:
        logger.error(
            f"Specified configuration json path {args.config_json_path} doesn't exist.Please validate and rerun.Exiting..")
        exit(-100)
    source_id = args.source_id
    infoworks.sdk.local_configurations.REQUEST_TIMEOUT_IN_SEC = 30
    infoworks.sdk.local_configurations.MAX_RETRIES = 3
    iwx_client = InfoworksClientSDK()
    iwx_client.initialize_client_with_defaults(configuration_json.get('protocol', 'http'),
                                               configuration_json.get('host', 'localhost'),
                                               configuration_json.get('port', '3001'),
                                               configuration_json.get("refresh_token",""))
    workflow_obj = BulkWorkflow(source_id,iwx_client,configuration_json)
    workflow_obj.prepare_domain_name_to_id_mappings()
    domain_name = configuration_json.get("domain_name","")
    domain_id = workflow_obj.domains_name_to_id_mappings.get(domain_name.lower(), None)
    workflow_obj.prepare_pipeline_name_to_id_mappings(domain_id)
    workflow_obj.bulk_workflow_creation()
    logger.info("Process completed successfully!")
if __name__ == '__main__':
    main()