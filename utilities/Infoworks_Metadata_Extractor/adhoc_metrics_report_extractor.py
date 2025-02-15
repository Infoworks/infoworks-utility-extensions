import csv
import logging
import traceback
import warnings
import pandas as pd
import os
import json
import subprocess
import sys
import re
from collections import defaultdict
import pkg_resources
import argparse
import inspect
from typing import List

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s — [ %(levelname)s ] — %(message)s",
                    datefmt='%d-%b-%y %H:%M:%S',
                    )


class AdhocMetricsReport:
    def __init__(self, iwx_client, output_directory):
        self.iwx_client = iwx_client
        self.output_directory = output_directory
        self.extension_reference_in_workflow_dict = defaultdict(list)

    def dataframe_writer(self, dataframe, report_type):
        try:
            file_path = self.output_directory + report_type + ".csv"
            print(f"Writing the data frame to file {file_path}")
            dataframe.to_csv(file_path, index=False)
            print("Report generated Successfully!")
        except Exception as e:
            print(str(e))
            raise Exception("Error generating report")

    def extract_workflow_with_bash_node_with_last_runtime(self):
        response = self.iwx_client.list_domains()
        domains = response["result"]["response"]["result"]
        workflow_with_bash_nodes = []
        for domain in domains:
            workflows = self.iwx_client.get_list_of_workflows(domain_id=domain["id"])
            workflows = workflows.get("result", {}).get("response", {}).get("result", [])
            for workflow in workflows:
                tasks = workflow.get("workflow_graph", {}).get("tasks", {})
                for task in tasks:
                    if task["task_type"] == "bash_command_run":
                        temp = {}
                        temp["domain_id"] = workflow.get("domain_id", "")
                        temp["domain_name"] = domain.get("name", "")
                        temp["workflow_id"] = workflow.get("id", "")
                        temp["workflow_name"] = workflow.get("name", "")
                        temp["created_at"] = workflow.get("created_at", "")
                        temp["created_by"] = workflow.get("created_by", "")
                        temp["modified_at"] = workflow.get("modified_at", "")
                        temp["modified_by"] = workflow.get("modified_by", "")
                        temp["bash_node"] = task
                        temp["custom_image_url"] = task.get("task_properties", {}).get("advanced_configurations",
                                                                                       {}).get("k8_image_url", "")
                        bash_command = task.get("task_properties", {}).get("bash_command", "")
                        matches = re.findall("\/opt\/infoworks\/uploads\/extensions\/.*\/.*\.\w+", bash_command)
                        # If match is not None, the pattern was found
                        if matches:
                            for match in matches:
                                self.extension_reference_in_workflow_dict[match].append(workflow.get("name", ""))
                        else:
                            pass
                        workflow_latest_run = self.iwx_client.get_list_of_workflow_runs(domain_id=domain["id"],
                                                                                        workflow_id=workflow["id"],
                                                                                        params={"limit": 1,
                                                                                                "sort_by": "created_at",
                                                                                                "order_by": "desc"},
                                                                                        pagination=False)
                        workflow_latest_run = workflow_latest_run.get("result", {}).get("response", {}).get("result",
                                                                                                            [])
                        if workflow_latest_run:
                            workflow_latest_run = workflow_latest_run[0]
                            temp["workflow_latest_run_id"] = workflow_latest_run["id"]
                            temp["workflow_latest_run_time"] = workflow_latest_run["created_at"]
                        workflow_with_bash_nodes.append(temp)
        workflow_with_bash_nodes_df = pd.DataFrame(workflow_with_bash_nodes)
        self.dataframe_writer(dataframe=workflow_with_bash_nodes_df,
                              report_type="extract_workflow_with_bash_node_with_last_runtime")

    def extract_job_hook_usage(self):
        response = self.iwx_client.list_job_hooks()
        job_hooks = response["result"]["response"]["result"]
        job_hook_dependencies_list = []
        for job_hook in job_hooks:
            temp = {}
            job_hook_id = job_hook["id"]["$oid"]
            temp["job_hook_id"] = job_hook_id
            temp["job_hook_name"] = job_hook["name"]
            temp["execution_type"] = job_hook["execution_type"]
            temp["executable_file_name"] = job_hook["executable_file_name"]
            response = self.iwx_client.get_list_of_job_hook_dependencies(job_hook_id=job_hook_id)
            job_hook_dependencies = response.get("result", {}).get("response", {}).get("result", [])
            temp["sources_using_job_hook"] = job_hook_dependencies["sources"]
            temp["pipelines_using_job_hook"] = job_hook_dependencies["pipelines"]
            workflows_using_job_hook = []
            for extension in self.extension_reference_in_workflow_dict.keys():
                match = re.search(f"\/opt\/infoworks\/uploads\/extensions\/{job_hook_id}\/.*\.\w+", extension)
                if match:
                    workflows_using_job_hook.extend(self.extension_reference_in_workflow_dict[extension])
            if temp.get("workflows_using_job_hook", []) == []:
                temp["workflows_using_job_hook"] = workflows_using_job_hook
            else:
                temp["workflows_using_job_hook"] = temp["workflows_using_job_hook"].extend(workflows_using_job_hook)
            temp["file_details"] = job_hook.get("file_details", [])
            job_hook_dependencies_list.append(temp)
        job_hook_dependencies_df = pd.DataFrame(job_hook_dependencies_list)
        self.dataframe_writer(job_hook_dependencies_df, report_type=inspect.stack()[0][3])

    def extract_domain_info(self):
        domains_response = self.iwx_client.list_domains()
        domains = domains_response.get('result', {}).get('response', {}).get('result', [])
        parsed_domain_data = []
        user_data = {}
        for domain in domains:
            user_id = domain.get('created_by')
            if user_id:
                if user_id in user_data:
                    user_name = user_data[user_id]
                else:
                    user_details_response = self.iwx_client.get_user_details(user_id=user_id)
                    user_result = user_details_response.get('result', {}).get('response', {}).get('result', [])
                    if isinstance(user_result[0], dict):
                        user_name = user_result[0]['profile']['name']
                    else:
                        logging.info(f"User Details not found for id '{user_id}' - Response : {user_details_response}")
                        user_name = ''
                    user_data[user_id] = user_name
            else:
                user_name = ''
            domain_row = {
                "domain_id": domain['id'],
                "domain_name": domain['name'],
                "description": domain.get('description'),
                "created_by": user_name
            }
            parsed_domain_data.append(domain_row)
        domains_df = pd.DataFrame(parsed_domain_data)
        self.dataframe_writer(domains_df, report_type=inspect.stack()[0][3])

    def extract_user_info(self):
        try:
            user_data=[]
            domains_response = self.iwx_client.list_domains()
            domains = domains_response.get('result', {}).get('response', {}).get('result', [])
            domains_dict = {domain['id']: domain['name'] for domain in domains}

            users_response = self.iwx_client.list_users()
            users = users_response.get('result', {}).get('response', {}).get('result', [])
            for user in users:
                accessible_domains = user.get('accessible_domains', [])
                domain_names_list = [domains_dict.get(domain_id, '') for domain_id in accessible_domains if
                                     domains_dict.get(domain_id)]
                accessible_domains_names = ', '.join(domain_names_list)
                user_row = {"id": user['id'], "profile.name": user['profile'].get('name'),
                            "profile.email": user['profile'].get('email'), "roles": user.get('roles'),
                            "accessible_domains": accessible_domains_names, "created_at": user.get('created_at'),
                            "status.last_login_details": user.get('status', {}).get('last_login_details')}
                user_data.append(user_row)
            users_df = pd.DataFrame(user_data)
            self.dataframe_writer(users_df, report_type=inspect.stack()[0][3])
        except Exception as error:
            raise Exception(f"Failed to get users info: {error}")

    def extract_infoworks_artifact_creator(self):
        response = self.iwx_client.get_list_of_sources()
        sources = response["result"]["response"]["result"]
        users = self.iwx_client.list_users()
        users = users.get("result", {}).get("response", {}).get("result", [])
        users_lookup = {}
        for user in users:
            users_lookup[user["id"]] = (
                user.get("profile", {}).get("name", ""), user.get("profile", {}).get("email", ""))
        artifact_creator_list = []
        for source in sources:
            temp = {}
            temp["artifact_id"] = source["id"]
            temp["artifact_name"] = source["name"]
            temp["artifact_type"] = "source"
            temp["creator_name"], temp["creator_email"] = users_lookup.get(source.get("created_by", ""),("deleted_user","deleted_user"))
            temp["created_at"] = source.get("created_at")
            temp["created_by"] = source.get("created_by")
            temp["modified_at"] = source.get("modified_at")
            temp["modified_by"] = source.get("modified_by")
            artifact_creator_list.append(temp)
        response = self.iwx_client.list_domains()
        domains = response["result"]["response"]["result"]
        for domain in domains:
            pipelines_under_domain = self.iwx_client.list_pipelines(domain_id=domain["id"])
            pipelines_under_domain = pipelines_under_domain.get("result", {}).get("response", {}).get("result", [])
            for pipeline in pipelines_under_domain:
                temp = {}
                temp["artifact_id"] = pipeline["id"]
                temp["artifact_name"] = pipeline["name"]
                temp["artifact_type"] = "pipeline"
                temp["domain_name"] = domain["name"]
                temp["creator_name"], temp["creator_email"] = users_lookup.get(pipeline.get("created_by", ""),("deleted_user","deleted_user"))
                temp["created_at"] = pipeline.get("created_at")
                temp["created_by"] = pipeline.get("created_by")
                temp["modified_at"] = pipeline.get("modified_at")
                temp["modified_by"] = pipeline.get("modified_by")
                artifact_creator_list.append(temp)
            workflows_under_domain = self.iwx_client.get_list_of_workflows(domain_id=domain["id"])
            workflows_under_domain = workflows_under_domain.get("result", {}).get("response", {}).get("result", [])
            for workflow in workflows_under_domain:
                temp = {}
                temp["artifact_id"] = workflow["id"]
                temp["artifact_type"] = "workflow"
                temp["creator_name"], temp["creator_email"] = users_lookup.get(
                    workflow.get("created_by", ("deleted_user", "deleted_user")), ("deleted_user", "deleted_user"))
                temp["created_at"] = workflow.get("created_at")
                temp["created_by"] = workflow.get("created_by")
                temp["modified_at"] = workflow.get("modified_at")
                temp["modified_by"] = workflow.get("modified_by")
                artifact_creator_list.append(temp)

        extract_infoworks_artifact_creator_df = pd.DataFrame(artifact_creator_list)
        self.dataframe_writer(extract_infoworks_artifact_creator_df, report_type=inspect.stack()[0][3])

    def extract_extension_report(self):
        extract_extensions_list = []
        databricks_envs_response = self.iwx_client.get_environment_details(
            params={"filter": {"data_warehouse_type": {"$exists": False}}})
        databricks_envs = databricks_envs_response.get("result", {}).get("response", {}).get("result", [])
        databricks_envs_ids = [env["id"] for env in databricks_envs]
        domains = self.iwx_client.list_domains(params={"filter": {"environment_ids": {"$in": databricks_envs_ids}}})
        domains = domains.get("result", {}).get("response", {}).get("result", [])
        extension_usage_list = []
        for domain in domains:
            pipeline_extensions = self.iwx_client.get_pipeline_extensions_associated_with_domain(domain_id=domain["id"])
            pipeline_extensions = pipeline_extensions.get("result", {}).get("response", {}).get("result", [])
            pipelines = self.iwx_client.list_pipelines(domain_id=domain["id"],
                                                       params={"filter": {"run_job_on_data_plane": True}})
            pipelines = pipelines.get("result", {}).get("response", {}).get("result", [])
            for pipeline in pipelines:
                pipeline_active_version_id = pipeline.get("active_version_id", None)
                if pipeline_active_version_id is not None:
                    pipeline_version_details = self.iwx_client.get_pipeline_version_details(domain_id=domain["id"],
                                                                                            pipeline_id=pipeline["id"],
                                                                                            pipeline_version_id=
                                                                                            pipeline[
                                                                                                "active_version_id"])
                    pipeline_version_details = pipeline_version_details.get("result", {}).get("response", {}).get(
                        "result", {})
                    for node_name, node in pipeline_version_details.get("model", {}).get("nodes", {}).items():
                        if node_name.upper().startswith("CUSTOM_TARGET"):
                            extension_id = node.get("properties", {}).get("extension_id", "")
                            extension_usage_list.append(
                                {"pipeline_id": pipeline["id"], "pipelines_using_extension": pipeline["name"],
                                 "domain_name": domain["name"], "extension_id": extension_id,
                                 "sources_using_extension": []})
            extract_extensions_list.extend(pipeline_extensions)
        source_extensions = self.iwx_client.list_source_extensions()
        source_extension_list = source_extensions.get("result", {}).get("response", {}).get("result", [])
        extract_extensions_list.extend(source_extension_list)
        for source_extension in source_extension_list:
            source_extension_id = source_extension["id"]
            dependencies = self.iwx_client.get_list_of_source_extension_dependencies(
                source_extension_id=source_extension_id)
            dependencies = dependencies.get("result", {}).get("response", {}).get("result", [])
            extension_usage_list.append(
                {"extension_id": source_extension_id, "sources_using_extension": dependencies.get("sources", [])})
        extract_extensions_df = pd.DataFrame(extract_extensions_list)
        extension_usage_df = pd.DataFrame(extension_usage_list,
                                          columns=["pipeline_id", "pipelines_using_extension", "domain_name",
                                                   "extension_id", "sources_using_extension"])
        extension_usage_df["sources_using_extension"] = extension_usage_df["sources_using_extension"].apply(
            lambda x: ', '.join(map(str, x)) if isinstance(x, list) else "")
        extension_usage_df = extension_usage_df.groupby(["extension_id"])[
            ['pipelines_using_extension', 'domain_name', 'sources_using_extension']].agg(set)
        extension_usage_df["domain_name"] = extension_usage_df["domain_name"].apply(lambda x: list(x))
        extension_usage_df["pipelines_using_extension"] = extension_usage_df["pipelines_using_extension"].apply(
            lambda x: list(x))
        extension_usage_df["sources_using_extension"] = extension_usage_df["sources_using_extension"].apply(
            lambda x: list(x))
        print(extension_usage_df)
        print("extension_usage_df.columns")
        print(extension_usage_df.columns)
        print("extract_extensions_df.columns")
        print(extract_extensions_df.columns)
        if not extract_extensions_df.empty:
            resultant_df = extract_extensions_df.merge(extension_usage_df, how="left", left_on="id",
                                                       right_on="extension_id")
            resultant_df["extension_name"] = resultant_df["pipeline_extension_name"].fillna(resultant_df["name"])
            resultant_df.drop(columns=['pipeline_extension_name', 'name'], inplace=True)
            extension_name_column = resultant_df.pop("extension_name")
            # Insert the column at the desired position
            resultant_df.insert(1, "extension_name", extension_name_column)
            self.dataframe_writer(resultant_df, report_type=inspect.stack()[0][3])
        else:
            print("No Extensions Usage Found!")

    def extract_target_data_connections_report(self):
        pipeline_parsed_count = 0
        response = self.iwx_client.get_data_connection()
        target_data_connections = response.get("result", {}).get("response", {}).get("result", [])
        target_data_connections_df = pd.DataFrame(target_data_connections)
        databricks_envs_response = self.iwx_client.get_environment_details(
            params={"filter": {"data_warehouse_type": {"$exists": False}}})
        databricks_envs = databricks_envs_response.get("result", {}).get("response", {}).get("result", [])
        databricks_envs_ids = [env["id"] for env in databricks_envs]
        domains = self.iwx_client.list_domains(params={"filter": {"environment_ids": {"$in": databricks_envs_ids}}})
        # domains = self.iwx_client.list_domains()
        domains = domains.get("result", {}).get("response", {}).get("result", [])
        pipelines_using_extension = []
        for domain in domains:
            pipelines = self.iwx_client.list_pipelines(domain_id=domain["id"])
            pipelines = pipelines.get("result", {}).get("response", {}).get("result", [])
            for pipeline in pipelines:
                pipeline_parsed_count += 1
                pipeline_active_version_id = pipeline.get("active_version_id", None)
                if pipeline_active_version_id is not None:
                    pipeline_version_details = self.iwx_client.get_pipeline_version_details(domain_id=domain["id"],
                                                                                            pipeline_id=pipeline["id"],
                                                                                            pipeline_version_id=pipeline_active_version_id)
                    pipeline_version_details = pipeline_version_details.get("result", {}).get("response", {}).get(
                        "result",
                        {})
                    for node_name, node in pipeline_version_details.get("model", {}).get("nodes", {}).items():
                        if "TARGET" in node_name.upper():
                            data_connection_id = node.get("properties", {}).get("data_connection_id", "")
                            if data_connection_id != "":
                                pipelines_using_extension.append(
                                    {"pipeline_id": pipeline["id"], "pipelines_using_data_connection": pipeline["name"],
                                     "domain_name": domain["name"], "data_connection_id": data_connection_id})
        if pipelines_using_extension:
            pipelines_using_extension_df = pd.DataFrame(pipelines_using_extension)
            pipelines_using_extension_df = pipelines_using_extension_df.groupby(["data_connection_id"])[[
                'pipelines_using_data_connection', 'domain_name']].agg(set)
            pipelines_using_extension_df["domain_name"] = pipelines_using_extension_df["domain_name"].apply(
                lambda x: list(x))
        else:
            pipelines_using_extension_df = pd.DataFrame(columns=['data_connection_id',
                                                                 'pipelines_using_data_connection', 'domain_name'])
        sources = self.iwx_client.get_list_of_sources(
            params={"filter": {"environment_id": {"$in": databricks_envs_ids}}})
        sources = sources.get("result", {}).get("response", {}).get("result", [])
        sources_using_target_connection = []
        for source in sources:
            tables = self.iwx_client.list_tables_under_source(source_id=source["id"], params={
                "filter": {"export_configuration": {"$exists": True}}})
            tables = tables.get("result", {}).get("response", {}).get("result", [])
            data_connection_id = None
            for table in tables:
                tables_using_dataconnections_under_this_source = []
                if table["export_configuration"].get("connection", "") != "":
                    data_connection_id = table["export_configuration"].get("connection", {}).get("data_connection_id",
                                                                                                 "")
                    if data_connection_id:
                        tables_using_dataconnections_under_this_source.append(table["name"])
            if data_connection_id:
                sources_using_target_connection.append({
                    "source_tables_using_extension": (source["name"], tables_using_dataconnections_under_this_source),
                    "data_connection_id": data_connection_id
                })
        if sources_using_target_connection:
            sources_using_target_connection_df = pd.DataFrame(sources_using_target_connection)
        else:
            sources_using_target_connection_df = pd.DataFrame(columns=['data_connection_id',
                                                                       'source_tables_using_extension'])
        resultant_df = target_data_connections_df.merge(pipelines_using_extension_df, how="left", left_on="id",
                                                        right_on="data_connection_id")
        resultant_df = resultant_df.merge(sources_using_target_connection_df, how="left", left_on="id",
                                          right_on="data_connection_id")
        print(resultant_df)
        self.dataframe_writer(resultant_df, report_type=inspect.stack()[0][3])
        print("total pipeline parsed : ", pipeline_parsed_count)

    def extract_generic_source_types_usage_report(self):
        response = self.iwx_client.list_generic_source_types()
        generic_source_types = response["result"]["response"]["result"]
        generic_source_types_dependencies_list = []
        for generic_source in generic_source_types:
            sources_dependencies = []
            temp = {}
            generic_source_dependencies = self.iwx_client.get_list_of_generic_source_type_dependencies(
                generic_source_type_id=generic_source["id"])
            generic_source_dependencies = generic_source_dependencies["result"]["response"]["result"]
            sources_dependencies = [artifact["name"] for artifact in generic_source_dependencies]
            temp["name"] = generic_source["name"]
            temp["id"] = generic_source["id"]
            temp["dependent_sources"] = sources_dependencies
            temp["created_at"] = generic_source.get("created_at")
            temp["created_by"] = generic_source.get("created_by")
            temp["modified_at"] = generic_source.get("modified_at")
            temp["modified_by"] = generic_source.get("modified_by")
            generic_source_types_dependencies_list.append(temp)
        extract_generic_source_types_dependencies_df = pd.DataFrame(generic_source_types_dependencies_list)
        self.dataframe_writer(extract_generic_source_types_dependencies_df, report_type=inspect.stack()[0][3])

    def extract_admin_schedules_report(self):
        response = self.iwx_client.list_schedules_as_admin()
        admin_schedules = response["result"]["response"]["result"]
        domains = self.iwx_client.list_domains()
        domains = domains["result"]["response"]["result"]
        domains_name_look_up = {}
        for domain in domains:
            domains_name_look_up[domain["id"]] = domain["name"]
        print(domains_name_look_up)
        extract_admin_schedules_report_list = []
        for schedule in admin_schedules:
            temp = schedule.copy()
            if schedule.get("parent_id", None):
                parent_id = schedule.get("parent_id", None)
                domain_name = domains_name_look_up.get(parent_id, None)
                if domain_name:
                    temp["domain_name"] = domain_name
            extract_admin_schedules_report_list.append(temp)
        extract_admin_schedules_report_df = pd.DataFrame(extract_admin_schedules_report_list)
        self.dataframe_writer(extract_admin_schedules_report_df, report_type=inspect.stack()[0][3])

    def extract_saml_report(self):
        response = self.iwx_client.list_auth_configs()
        saml_config = [config for config in response["result"]["response"]["result"] if
                       config['authentication_type'] == 'saml']
        if len(saml_config) > 0:
            try:
                csv_file = self.output_directory + "extract_saml_report.csv"
                header = saml_config[0].keys()
                with open(csv_file, 'w', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=header)
                    writer.writeheader()
                    for config in saml_config:
                        writer.writerow(config)
                print(f"SAML authentication configuration saved to {csv_file}")
            except Exception as e:
                traceback.print_exc()

    def extract_secrets_usage_report(self):
        secret_name_id_mapping = {}
        def prepare_secret_name_to_id_mapping(iwx_client):
            secrets = iwx_client.list_secrets()
            secrets = secrets.get("result", {}).get("response", {}).get("result", [])
            for secret in secrets:
                secret_name_id_mapping[secret["name"]] = secret["id"]

        prepare_secret_name_to_id_mapping(self.iwx_client)

        def find_secret_id_in_nested_dict(iwx_client, data):
            for key, value in data.items():
                if isinstance(value, dict):
                    # If the value is a dictionary, recurse into it
                    secret_id = find_secret_id_in_nested_dict(iwx_client, value)
                    if secret_id:
                        return secret_id
                elif key == "secret_id":
                    return value
            return None
        try:
            errors = []
            sources = self.iwx_client.get_list_of_sources()
            sources = sources.get("result", {}).get("response", {}).get("result", [])
            domain_secrets = defaultdict(set)
            source_secrets = defaultdict(set)
            domain_envs = defaultdict(set)
            extract_secret_usage_report_list = []
            for source in sources:
                # print(json.dumps(source,indent=4))
                environment_id = source["environment_id"]
                source_connection = self.iwx_client.get_source_connection_details(source_id=source["id"])
                source_connection = source_connection.get("result", {}).get("response", {}).get("result", [])
                secret_id = find_secret_id_in_nested_dict(iwx_client=self.iwx_client, data=source_connection)
                if secret_id is not None:
                    for domain_id in source.get("associated_domains", []):
                        domain_secrets[domain_id].add(secret_id)
                        domain_envs[domain_id].add(environment_id)
                    source_secrets[secret_id].add(source["name"])
                tables = self.iwx_client.list_tables_under_source(source_id=source["id"], params={
                    "filter": {"export_configuration.sync_type": {"$exists": True, "$ne": "DISABLED"},
                               "export_configuration.connection.data_connection_id": {
                                   "$exists": False}
                               }})
                tables = tables.get("result", {}).get("response", {}).get("result", [])
                for table in tables:
                    # print(table)
                    secret_id = find_secret_id_in_nested_dict(iwx_client=self.iwx_client, data=table["export_configuration"])
                    if secret_id is not None:
                        for domain_id in source.get("associated_domains", []):
                            logging.info(f"secret_found:{secret_id}")
                            domain_secrets[domain_id].add(secret_id)
                            source_secrets[secret_id].add(source["name"])
            secrets = self.iwx_client.list_secrets()
            secrets = secrets.get("result", {}).get("response", {}).get("result", [])
            domains = self.iwx_client.list_domains_as_admin()
            domains = domains.get("result", {}).get("response", {}).get("result", [])
            domain_name_lookup = {}
            for domain in domains:
                domain_name_lookup[domain["id"]] =domain["name"]
            for secret in secrets:
                secret_details = self.iwx_client.get_secret_details(secret_id=secret["id"])
                secret_details = secret_details.get("result", {}).get("response", {}).get("result", {})
                secret["associated_domains"] = [domain_name_lookup[id] for id in secret_details["domains"]]
                secret["sources_using_this_secret"] = list(source_secrets[secret["id"]])
                secret_store_details = self.iwx_client.get_secret_store_details(secret_store_id=secret["secret_store"])
                secret_store_details = secret_store_details.get("result", {}).get("response", {}).get("result", {})
                secret["secret_store_details"] = secret_store_details
                secret["secret_store_name"] = secret_store_details.get("name", "")
                service_authentication = secret_store_details.get("service_authentication", "")
                if service_authentication:
                    service_authentication_details = self.iwx_client.get_service_authentication_details(service_auth_id=service_authentication)
                    service_authentication_details = service_authentication_details.get("result", {}).get("response", {}).get("result", {})
                    secret["service_authentication_details"] = service_authentication_details
                extract_secret_usage_report_list.append(secret)
        except Exception as e:
            logging.error(str(e))
            errors.append(str(e))
        finally:
            extract_secret_usage_report_df = pd.DataFrame(extract_secret_usage_report_list)
            self.dataframe_writer(extract_secret_usage_report_df, report_type=inspect.stack()[0][3])

def get_all_report_methods() -> List[str]:
    method_list = [func for func in dir(AdhocMetricsReport) if
                   callable(getattr(AdhocMetricsReport, func)) and not func.startswith("__") and func.startswith(
                       "extract")]
    return method_list


def main():
    required = {'pandas', 'infoworkssdk==5.0.5'}
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
    parser = argparse.ArgumentParser(description="Generate AdHoc Metrics Reports.")
    # parser.add_argument('--data-source', type=str, help='Data source path', required=True)
    parser.add_argument('--config_file', required=True, help='Fully qualified path of the configuration file')
    parser.add_argument('--output_directory', required=False, type=str,
                        help='Pass the directory where the reports are to be exported', default=f"{file_path}/csv/")
    parser.add_argument('--reports', nargs='+',
                        help='List of reports to generate. If not specified, all will be generated.',
                        default=get_all_report_methods(), choices=get_all_report_methods())
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
    report_generator = AdhocMetricsReport(iwx_client=iwx_client, output_directory=output_directory)
    report_methods = args.reports
    report_methods.sort(key=lambda x: getattr(AdhocMetricsReport, x).__code__.co_firstlineno)
    for report in report_methods:
        if hasattr(report_generator, report):
            method = getattr(report_generator, report)
            method()


if __name__ == "__main__":
    main()
