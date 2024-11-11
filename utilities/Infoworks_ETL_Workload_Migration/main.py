import configparser
import queue
import threading
import subprocess
import traceback
import sys
import os
import argparse
#sys.path.insert(0,"/Users/nitin.bs/PycharmProjects/infoworks-python-sdk/")
required = {'infoworkssdk==5.0.6'}
import pkg_resources
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing, '--user'], stdout=subprocess.DEVNULL)
    user_site = subprocess.run([python, "-m", "site", "--user-site"], capture_output=True, text=True)
    module_path = user_site.stdout.strip()
    sys.path.append(module_path)
from infoworks.sdk.client import InfoworksClientSDK
from infoworks.sdk import local_configurations
local_configurations.POLLING_TIMEOUT=120000
import urllib3
parser = argparse.ArgumentParser(description='Import Infoworks Artifacts')
parser.add_argument('--config_ini_file', required=True, type=str, help='Pass the config ini file path along with filename')
args = parser.parse_args()
config_ini_file=args.config_ini_file
if not os.path.exists(config_ini_file):
    print(f"Please provide valid config_ini_file parameter.{config_ini_file} doesn't exist.Exiting...")
    exit(-100)
urllib3.disable_warnings()
thisfolder = os.path.dirname(os.path.abspath(__file__))
initfile = config_ini_file
config = configparser.ConfigParser()
config.optionxform = str
config.read(initfile)
protocol = config.get("api_mappings", "protocol")
ip = config.get("api_mappings", "ip")
port = config.get("api_mappings", "port")
maintain_lineage = config.get("api_mappings", "maintain_lineage")
refresh_token=""
try:
    refresh_token = config.get("api_mappings", "refresh_token")
    if refresh_token == "":
        print("Refresh token is empty. Trying to pick it up from IWX_REFRESH_TOKEN environment variable")
        if os.getenv("IWX_REFRESH_TOKEN") is None:
            print("Could not find the refresh token in IWX_REFRESH_TOKEN env variable as well.Exiting")
            exit(-100)
        else:
            print("Found IWX_REFRESH_TOKEN env variable.Using refresh token from IWX_REFRESH_TOKEN env variable")
            refresh_token=os.getenv("IWX_REFRESH_TOKEN")
except configparser.NoOptionError:
    print("Refresh token is not available in config.ini.Trying to pick it up from IWX_REFRESH_TOKEN environment variable")
    if os.getenv("IWX_REFRESH_TOKEN") is None:
        print("Could not find the refresh token in IWX_REFRESH_TOKEN env variable as well.Exiting")
        exit(-100)
    else:
        print("Found IWX_REFRESH_TOKEN env variable.Using refresh token from IWX_REFRESH_TOKEN env variable")
        refresh_token = os.getenv("IWX_REFRESH_TOKEN")
        print(refresh_token)
env_tag = config.get("api_mappings", "env_tag")
iwx_client_prd = InfoworksClientSDK()
iwx_client_prd.initialize_client_with_defaults(protocol, ip, port, refresh_token)
iwx_client_prd.get_mappings_from_config_file(ini_config_file_path=initfile)

import networkx as nx

global_lock = threading.Lock()
num_fetch_threads = 10
job_queue = queue.Queue(maxsize=100)


def topological_sort_grouping(g):
    # copy the graph
    _g = g.copy()
    res = []
    # while _g is not empty
    while _g:
        zero_indegree = [v for v, d in _g.in_degree() if d == 0]
        res.append(zero_indegree)
        _g.remove_nodes_from(zero_indegree)
    return res


def execute(thread_number, q):
    while True:
        try:
            print('%s: Looking for the next task ' % thread_number)
            task = q.get()
            print(f'\nThread Number {thread_number} processed {task}')
            entity_type = task["entity_type"]
            if entity_type == "pipeline":
                print(f"Pipeline {task['pipeline_config_path']} in progress")
                iwx_client_prd.cicd_create_configure_pipeline(
                    configuration_file_path=task["pipeline_config_path"],
                    domain_name=task["domain_name"],
                    read_passwords_from_secrets=True,
                    env_tag=env_tag, secret_type="azure_keyvault" # pragma: allowlist-secret
                )
            elif entity_type == "source":
                print(f"Source {task['source_config_path']} in progress")
                res = iwx_client_prd.cicd_upload_source_configurations(
                    configuration_file_path=task["source_config_path"],
                    read_passwords_from_secrets=True,
                    env_tag=env_tag, secret_type="azure_keyvault" # pragma: allowlist-secret
                )
                # add in sdk 1.0.15
                # , config_ini_path = initfile
            elif entity_type == "workflow":
                print(f"Workflow {task['workflow_config_path']} in progress")
                iwx_client_prd.cicd_create_configure_workflow(configuration_file_path=task["workflow_config_path"],
                                                              domain_name=task["domain_name"])
            elif entity_type == "pipeline_group":
                print(f"Pipeline Group {task['pipeline_group_config_path']} in progress")
                iwx_client_prd.cicd_create_configure_pipeline_group(configuration_file_path=task["pipeline_group_config_path"],
                                                              domain_name=task["domain_name"])
            else:
                pass
        except Exception as e:
            print(str(e))
            print(traceback.format_exc())
        q.task_done()


if __name__ == '__main__':

    for i in range(num_fetch_threads):
        worker = threading.Thread(target=execute, args=(i, job_queue,))
        worker.setDaemon(True)
        worker.start()

    with open(os.path.join("configurations/modified_files", "source.csv"), "r") as src_files_fp:
        for src_file in src_files_fp.readlines():
            src_args = {"entity_type": "source",
                        "source_config_path": os.path.join("configurations/source", src_file.strip())}
            job_queue.put(src_args)
            print(src_args)
    print('*** Main thread waiting to complete all source configuration requests ***')
    job_queue.join()
    print('*** Done with Source Configurations ***')

    if maintain_lineage == 'true':
        graph = nx.read_gexf("configurations/modified_files/pipeline.gexf")
        for item in topological_sort_grouping(graph):
            for i in item:
                if i is not "root":
                    pipeline_file = graph._node[i]['filename'].strip()
                    domain_name = pipeline_file.split("#")[0]
                    domain_mappings = iwx_client_prd.mappings.get("domain_name_mappings", {})
                    if domain_mappings != {}:
                        if domain_name.lower() in domain_mappings.keys():
                            domain_name = domain_mappings.get(domain_name.lower(), domain_name)
                    pipeline_args = {"entity_type": "pipeline",
                                     "pipeline_config_path": os.path.join("configurations/pipeline", pipeline_file),
                                     "domain_name": domain_name}
                    job_queue.put(pipeline_args)
            print('*** Main thread waiting to complete all pending pipeline creation/configuration requests ***')
            job_queue.join()
            print('*** Done with All Tasks ***')
    else:
        with open(os.path.join("configurations/modified_files", "pipeline.csv"), "r") as pipeline_files_fp:
            overall_pipelines_report_list=[]
            for pipeline_file in pipeline_files_fp.readlines():
                pl_name = pipeline_file.strip()
                domain_name = pl_name.split("#")[0]
                domain_mappings = iwx_client_prd.mappings.get("domain_name_mappings", {})
                if domain_mappings != {}:
                    if domain_name.lower() in domain_mappings.keys():
                        domain_name = domain_mappings.get(domain_name.lower(), domain_name)
                pipeline_args = {"entity_type": "pipeline",
                                 "pipeline_config_path": os.path.join("configurations/pipeline", pl_name),
                                 "domain_name": domain_name}
                job_queue.put(pipeline_args)

        print('*** Main thread waiting to complete all pipeline configuration requests ***')
        job_queue.join()
        print('*** Done with Pipeline Configurations ***')

    with open(os.path.join("configurations/modified_files", "pipeline_group.csv"), "r") as pipeline_grp_file_fp:
        for pipeline_grp_file in pipeline_grp_file_fp.readlines():
            pipeline_grp_name = pipeline_grp_file.strip()
            domain_name = pipeline_grp_name.split("#")[0]
            domain_mappings = iwx_client_prd.mappings.get("domain_name_mappings", {})
            if domain_mappings != {}:
                if domain_name.lower() in domain_mappings.keys():
                    domain_name = domain_mappings.get(domain_name.lower(), domain_name)
            pipeline_grp_args = {"entity_type": "pipeline_group",
                        "pipeline_group_config_path": os.path.join("configurations/pipeline_group", pipeline_grp_file.strip()),
                                 "domain_name": domain_name}
            job_queue.put(pipeline_grp_args)
            print(pipeline_grp_args)
    print('*** Main thread waiting to complete all pipeline group configuration requests ***')
    job_queue.join()
    print('*** Done with pipeline group Configurations ***')

    if maintain_lineage == 'true':
        graph = nx.read_gexf("configurations/modified_files/workflow.gexf")
        for item in topological_sort_grouping(graph):
            for i in item:
                if i is not "root":
                    wf_name = graph._node[i]['filename'].strip()
                    domain_name = wf_name.split("#")[0]
                    domain_mappings = iwx_client_prd.mappings.get("domain_name_mappings", {})
                    if domain_mappings != {}:
                        if domain_name.lower() in domain_mappings.keys():
                            domain_name = domain_mappings.get(domain_name.lower(), domain_name)
                    wf_args = {"entity_type": "workflow",
                               "workflow_config_path": os.path.join("configurations/workflow", wf_name),
                               "domain_name": domain_name}
                    job_queue.put(wf_args)
            print('*** Main thread waiting to complete all pending pipeline creation/configuration requests ***')
            job_queue.join()
            print('*** Done with All Tasks ***')
    else:
        with open(os.path.join("configurations/modified_files", "workflow.csv"), "r") as workflow_file_fp:
            for workflow_file in workflow_file_fp.readlines():
                wf_name = workflow_file.strip()
                domain_name = wf_name.split("#")[0]
                domain_mappings = iwx_client_prd.mappings.get("domain_name_mappings", {})
                if domain_mappings != {}:
                    if domain_name.lower() in domain_mappings.keys():
                        domain_name = domain_mappings.get(domain_name.lower(), domain_name)
                wf_args = {"entity_type": "workflow",
                           "workflow_config_path": os.path.join("configurations/workflow", wf_name),
                           "domain_name": domain_name}
                job_queue.put(wf_args)

        print('*** Main thread waiting to complete all workflow configuration requests ***')
        job_queue.join()
        print('*** Done with Workflow Configurations  ***')
