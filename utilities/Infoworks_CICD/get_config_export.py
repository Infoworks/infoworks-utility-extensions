import argparse
import os
import subprocess
import sys
import json
import traceback
import pkg_resources
import urllib3
urllib3.disable_warnings()
#sys.path.insert(0,"/Users/nitin.bs/PycharmProjects/infoworks-python-sdk/")
required = {'infoworkssdk==5.0.6'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    python = sys.executable
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', *missing, '--user'], stdout=subprocess.DEVNULL)
    user_site = subprocess.run([python, "-m", "site", "--user-site"], capture_output=True, text=True)
    module_path = user_site.stdout.strip()
    sys.path.append(module_path)
print(sys.path)
from infoworks.sdk.client import InfoworksClientSDK
from infoworks.sdk.cicd.download_configurations.utils import Utils

def main():
    parser = argparse.ArgumentParser(description='Download Infoworks Artifacts')
    parser.add_argument('--refresh_token', required=True, type=str, help='Pass the refresh token')
    parser.add_argument('--protocol', required=True, type=str, help='Protocol for the API calls. http/https')
    parser.add_argument('--host', required=True, type=str, help='Rest API Host')
    parser.add_argument('--port', required=True, type=str, help='Rest API Port')
    parser.add_argument('--source_ids', required=False, type=str, help='Comma seperated source ids to export')
    parser.add_argument('--pipeline_ids', required=False, type=str, help='Comma seperated pipeline ids to export')
    parser.add_argument('--workflow_ids', required=False, type=str, help='Comma seperated workflow ids to export')
    parser.add_argument('--pipeline_group_ids', required=False, type=str, help='Comma seperated pipeline group ids to export')
    parser.add_argument('--source_name_regex', required=False, type=str, help='Source name regex to automatically get list of source ids based on regex')
    parser.add_argument('--pipeline_name_regex', required=False, type=str,
                        help='Pipeline name regex to automatically get list of pipeline ids based on regex')
    parser.add_argument('--workflow_name_regex', required=False, type=str,
                        help='Workflow name regex to automatically get list of workflow ids based on regex')
    parser.add_argument('--domain_name',required=False, type=str, help='Name of domain to search under for regex match of pipelines and workflows')
    parser.add_argument('--dump_watermarks',required=False, type=str, help='Dump watermark for tables True/False',default="False",choices=["True","False"])
    parser.add_argument("--custom_tag", type=str, required=False, default="{}", help="Custom tag as JSON string")
    parser.add_argument("--entity_type", type=str, required=False, default="all", help="entity type(source,pipeline,workflow)")
    args = parser.parse_args()
    # Connect to Dev environment and get the source export
    refresh_token = args.refresh_token
    iwx_client_dev = InfoworksClientSDK()
    iwx_client_dev.initialize_client_with_defaults(args.protocol, args.host, args.port, refresh_token)
    base_path = os.path.dirname(os.path.realpath(__file__))
    dump_watermarks = eval(args.dump_watermarks)
    pipeline_grp_ids=[]
    utils_obj = Utils("admin@infoworks.io")
    if args.pipeline_group_ids is not None:
        pipeline_grp_ids = args.pipeline_group_ids.split(",")
    pipeline_ids=[]
    print("args.custom_tag", args.custom_tag)
    custom_tag = json.loads(args.custom_tag)
    custom_tag_id = None
    key, value = None, None
    for k, v in custom_tag.items():
        key, value = k, v
    if (key is not None and value is not None):
        custom_tag_response = iwx_client_dev.get_list_of_custom_tags(
            params={"filter": {"$and": [{"key": f"{key}", "value": f"{value}"}]}})
        custom_tag_response = custom_tag_response["result"]["response"]["result"]
        if custom_tag_response:
            custom_tag_id = custom_tag_response[0]["id"]
        else:
            print(f"Error getting the ID for the custom tag {args.custom_tag}")
            print(custom_tag_response)
    try:
        if args.source_ids is not None:
            iwx_client_dev.cicd_get_sourceconfig_export(source_ids=args.source_ids.split(","),
                                                   config_file_export_path=os.path.join(base_path, "configurations"),dump_watermarks=dump_watermarks,custom_tag_id=custom_tag_id)
        elif args.source_name_regex is not None:
            sources_response = iwx_client_dev.get_list_of_sources(params={"filter": {"name": {"$regex": args.source_name_regex}}})
            sources = sources_response["result"]["response"]["result"]
            source_ids = [source["id"] for source in sources]
            iwx_client_dev.cicd_get_sourceconfig_export(source_ids=source_ids,
                                                        config_file_export_path=os.path.join(base_path,
                                                                                             "configurations"))
        elif custom_tag_id is not None and (args.entity_type == "source" or args.entity_type == "all"):
            sources_response = iwx_client_dev.get_list_of_sources(params={"filter": {"custom_tags":{"$in":[custom_tag_id]}}})
            sources = sources_response["result"]["response"]["result"]
            source_ids = [source["id"] for source in sources]
            iwx_client_dev.cicd_get_sourceconfig_export(source_ids=source_ids,
                                                        config_file_export_path=os.path.join(base_path,
                                                                                             "configurations"))
        else:
            file_to_truncate = open(os.path.join(base_path, "configurations", "modified_files", "source.csv"),
                                    'w')
            file_to_truncate.close()
    except Exception as e:
        print(f"Unable to download source configurations {str(e)}")
        print(traceback.print_exc())

    try:
        if len(pipeline_grp_ids)>0:
            file_to_truncate = open(os.path.join(base_path, "configurations", "modified_files", "pipeline_group.csv"),
                                    'w')
            file_to_truncate.close()
            file_to_truncate = open(os.path.join(base_path, "configurations", "modified_files", "pipeline.csv"),
                                    'w')
            file_to_truncate.close()
            for pipeline_grp_id in pipeline_grp_ids:
                pipeline_ids=[]
                json_obj = {"entity_id": pipeline_grp_id, "entity_type": "pipeline_group"}
                domain_id = utils_obj.get_domain_id(iwx_client_dev, json_obj)
                pipelines_group_details = iwx_client_dev.get_pipeline_group_details(domain_id, pipeline_grp_id)
                pipelines_group_details = pipelines_group_details["result"]["response"]["result"]
                environment_id = pipelines_group_details.get("environment_id",None)
                environment_details_response = iwx_client_dev.get_environment_details(environment_id=environment_id)
                environment_name = environment_details_response["result"]["response"]["result"][0]["name"]
                pipelines_group_details["environment_name"]=environment_name
                pipelines_list = pipelines_group_details["pipelines"]
                pipeline_tmp_ids = [pipeline["pipeline_id"] for pipeline in pipelines_list]
                pipeline_ids.extend(pipeline_tmp_ids)
                iwx_client_dev.cicd_get_pipelineconfig_export(pipeline_ids=pipeline_ids,
                                                     config_file_export_path=os.path.join(base_path, "configurations"),pipeline_grp_config=pipelines_group_details,files_overwrite=False)
        if args.pipeline_ids is not None:
            iwx_client_dev.cicd_get_pipelineconfig_export(pipeline_ids=args.pipeline_ids.split(","),
                                                         config_file_export_path=os.path.join(base_path,
                                                                                            "configurations"),pipeline_grp_config=None)
        elif args.pipeline_name_regex is not None:
            if args.domain_name is None:
                print("Domain name cannot be None for pipeline regex match")
                exit(-100)
            else:
                domain_response = iwx_client_dev.get_domain_id(args.domain_name)
                domain_id = domain_response["result"]["response"]["domain_id"]
                pipelines_response = iwx_client_dev.list_pipelines(domain_id=domain_id, params={
                    "filter": {"name": {"$regex": args.pipeline_name_regex}}})
                pipelines = pipelines_response["result"]["response"]["result"]
                pipeline_ids = [pipeline["id"] for pipeline in pipelines]
                iwx_client_dev.cicd_get_pipelineconfig_export(pipeline_ids=pipeline_ids,
                                                              config_file_export_path=os.path.join(base_path,
                                                                                                   "configurations"),
                                                              pipeline_grp_config=None)
        elif custom_tag_id is not None and (args.entity_type == "pipeline" or args.entity_type == "all"):
            if args.domain_name is None:
                print("Domain name cannot be None for exporting pipelines based on tags")
                exit(-100)
            else:
                domain_response = iwx_client_dev.get_domain_id(args.domain_name)
                domain_id = domain_response["result"]["response"]["domain_id"]
                pipelines_response = iwx_client_dev.list_pipelines(domain_id=domain_id, params={
                    "filter": {"custom_tags": {"$in": [custom_tag_id]}}})
                pipelines = pipelines_response["result"]["response"]["result"]
                pipeline_ids = [pipeline["id"] for pipeline in pipelines]
                iwx_client_dev.cicd_get_pipelineconfig_export(pipeline_ids=pipeline_ids,
                                                              config_file_export_path=os.path.join(base_path,
                                                                                                   "configurations"),
                                                              pipeline_grp_config=None)
        else:
            pass
        if args.pipeline_ids is None and len(pipeline_grp_ids)==0 and args.pipeline_name_regex is None:
            file_to_truncate = open(os.path.join(base_path, "configurations", "modified_files", "pipeline.csv"), 'w')
            file_to_truncate.close()
            file_to_truncate = open(os.path.join(base_path, "configurations", "modified_files", "pipeline_group.csv"),
                                    'w')
            file_to_truncate.close()
        elif len(pipeline_grp_ids)==0:
            file_to_truncate = open(os.path.join(base_path, "configurations", "modified_files", "pipeline_group.csv"), 'w')
            file_to_truncate.close()
        else:
            pass
    except Exception as e:
        print(f"Unable to download pipeline configurations {str(e)}")
        print(traceback.print_exc())

    try:
        if args.workflow_ids is not None:
            iwx_client_dev.cicd_get_workflowconfig_export(workflow_ids=args.workflow_ids.split(","),
                                                     config_file_export_path=os.path.join(base_path, "configurations"))
        elif args.workflow_name_regex is not None:
            if args.domain_name is None:
                print("Domain name cannot be None for workflow regex match")
                exit(-100)
            else:
                domain_response = iwx_client_dev.get_domain_id(args.domain_name)
                domain_id = domain_response["result"]["response"]["domain_id"]
                workflows_response = iwx_client_dev.get_list_of_workflows(domain_id=domain_id, params={
                    "filter": {"name": {"$regex": args.workflow_name_regex}}})
                workflows = workflows_response["result"]["response"]["result"]
                workflow_ids = [workflow["id"] for workflow in workflows]
                iwx_client_dev.cicd_get_workflowconfig_export(workflow_ids=workflow_ids,
                                                              config_file_export_path=os.path.join(base_path,
                                                                                                   "configurations"))

        elif custom_tag_id is not None and (args.entity_type == "workflow" or args.entity_type == "all"):
            if args.domain_name is None:
                print("Domain name cannot be None for workflow regex match")
                exit(-100)
            else:
                domain_response = iwx_client_dev.get_domain_id(args.domain_name)
                domain_id = domain_response["result"]["response"]["domain_id"]
                workflows_response = iwx_client_dev.get_list_of_workflows(domain_id=domain_id, params={
                    "filter": {"custom_tags": {"$in": [custom_tag_id]}}})
                workflows = workflows_response["result"]["response"]["result"]
                workflow_ids = [workflow["id"] for workflow in workflows]
                iwx_client_dev.cicd_get_workflowconfig_export(workflow_ids=workflow_ids,
                                                              config_file_export_path=os.path.join(base_path,
                                                                                                   "configurations"))
        else:
            file_to_truncate = open(os.path.join(base_path, "configurations","modified_files","workflow.csv"), 'w')
            file_to_truncate.close()
    except Exception as e:
        print(f"Unable to download workflow configurations {str(e)}")
        print(traceback.print_exc())


if __name__ == '__main__':
    main()