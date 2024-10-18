# Usage:
# python create_admin_entities_in_bulk.py --protocol <http/https> --host <hostname> --port <443/3000> --refresh_token <Infoworks admin user refresh token> --metadata_csv_path <path to metadata csv> --admin_entity_type <environment/secret/domain>
import argparse
import ast
import copy
import os
import subprocess
import sys
import pandas as pd
import traceback
from abc import ABC, abstractmethod
import json
import csv
import pkg_resources

cwd = os.path.dirname(os.path.realpath(__file__))
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
required = {'infoworkssdk==5.0.5'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    python = sys.executable
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
from infoworks.sdk.client import InfoworksClientSDK


class AdminEntity(ABC):

    def merge_dicts(self, dict1, dict2):
        merged_dict = dict1.copy()  # Start with the keys and values of dict1
        for key, value in dict2.items():
            if key in merged_dict:
                if isinstance(merged_dict[key], list) and isinstance(value, list):
                    # If both values are lists, merge and dedupe
                    merged_dict[key] = list(set(merged_dict[key] + value))
                elif isinstance(merged_dict[key], dict) and isinstance(value, dict):
                    # If both values are dicts, merge them recursively
                    merged_dict[key] = self.merge_dicts(merged_dict[key], value)
                else:
                    # Otherwise, override the value from dict2
                    merged_dict[key] = value
            else:
                merged_dict[key] = value
        return merged_dict

    @abstractmethod
    def get_existing_entity_id(self, entity_name):
        pass

    @abstractmethod
    def create_entity(self, configuration_body):
        pass

    @abstractmethod
    def update_entity_body_as_per_csv(self):
        pass


class ProfileEntity(AdminEntity):
    def __init__(self, iwx_client, metadata_csv_path, environment_id):
        self.iwx_client = iwx_client
        self.metadata_csv_path = metadata_csv_path
        self.environment_id = environment_id

    def validate_csv_metadata_schema(self):
        df = pd.read_csv(self.metadata_csv_path)
        columns_in_csv = list(df.columns)
        columns_in_csv = [column.lower() for column in columns_in_csv]
        expected_schema = ["name", "is_default_profile", "warehouse", "username", "secretname", "additional_params",
                           "session_params"]
        columns_diff = set(columns_in_csv) - set(expected_schema)
        if len(columns_diff) >= 0:
            print("CSV metadata schema matches the expected schema")
        else:
            print(
                f"Schema validation failed. Expected schema is {expected_schema} but got schema {columns_in_csv}.Exiting...")
            exit(-100)

    def get_existing_entity_id(self, entity_name):
        # et_environment_details = self.iwx_client.get_environment_id_from_name(environment_name=entity_name)
        # environment_id = get_environment_details.get("result", {}).get("response", {}).get("environment_id", None)
        environment_id_response = self.iwx_client.get_environment_details(
            params={"filter": {"name": entity_name}, "limit": 500, "fetch_all": True})
        if environment_id_response.get('result', {}).get('response', {}).get('result', []):
            environment_id = environment_id_response.get('result', {}).get('response', {}).get('result', [])[0]['id']
        else:
            environment_id = None

        return environment_id

    def create_entity(self, configuration_body):
        environment_id = self.environment_id
        if environment_id is None:
            env_creation_response = self.iwx_client.create_environment(configuration_body)
            environment_id = env_creation_response.get("result", {}).get("response", {}).get("result", {}).get("id")
            print(f"Create Environment Response: {env_creation_response}")
            if environment_id:
                return "success", env_creation_response
            else:
                return "failed", env_creation_response
        else:
            env_updation_response = self.iwx_client.update_environment(environment_id=environment_id,
                                                                       environment_body=configuration_body)
            print(env_updation_response.get("result", {}).get("response", {}))
            if env_updation_response.get('result', {}).get('status', '') == "success":
                return "success", env_updation_response
            else:
                return "failed", env_updation_response

    def update_entity_body_as_per_csv(self):
        self.validate_csv_metadata_schema()
        with open(self.metadata_csv_path, "r") as csv_file:
            reader = csv.DictReader(csv_file)
            snowflake_profile = []
            columns = ["profile_name", "status", "create_update_response"]
            overall_profile_status = pd.DataFrame(columns=columns)
            for row in reader:
                temp = {
                    "name": "Default",
                    "is_default_profile": False,
                    "warehouse": [],
                    "authentication_properties": {
                        "type": "default",
                        "username": "",
                        "password": {
                        }
                    }
                }
                if row.get("name", ""):
                    temp["name"] = row.get("name", "")
                if row.get("is_default_profile", ""):
                    temp["is_default_profile"] = json.loads(row.get("is_default_profile", "false"))
                if row.get("warehouse", []):
                    temp["warehouse"] = json.loads(row.get("warehouse", "[]"))
                if row.get("additional_params", []):
                    temp["additional_params"] = json.loads(row.get("additional_params", "[]"))
                if row.get("session_params", []):
                    temp["session_params"] = json.loads(row.get("session_params", "[]"))
                if row.get("username", ""):
                    temp["authentication_properties"]["username"] = row.get("username", "")
                if row.get("secretname", ""):
                    secret_name = row.get("secretname", "")
                    secret_res = self.iwx_client.list_secrets(params={"filter": {"name": secret_name}})
                    secret_res = secret_res.get("result", {}).get("response", {}).get("result", [])
                    secret_id = None
                    if secret_res:
                        secret_id = secret_res[0]["id"]
                    else:
                        print(f"secret {secret_name} not found.Exiting...")
                        exit(-100)
                    if secret_id:
                        temp["authentication_properties"]["password"][
                            "password_type"] = "secret_store"  # pragma: allowlist secret
                        temp["authentication_properties"]["password"]["secret_id"] = secret_id
                snowflake_profile.append(temp)
        env_configuration_response = self.iwx_client.get_environment_details(environment_id=self.environment_id)
        env_configuration_json = env_configuration_response.get("result", {}).get("response", {}).get("result", {})
        if env_configuration_json:
            env_configuration_json = env_configuration_json[0]
        else:
            print("Failed while getting exiting env details")
            print(env_configuration_response)
        old_profile_list = env_configuration_json.get("data_warehouse_configuration", {}).get("snowflake_profiles", [])
        old_profile_names_index = {}
        old_profile_names = []
        for i, old_profile in enumerate(old_profile_list):
            old_profile_names_index[old_profile["name"]] = i
            old_profile_names.append(old_profile["name"])
        new_profile_list = copy.deepcopy(old_profile_list)
        for i, profile in enumerate(snowflake_profile):
            if profile["name"] not in old_profile_names:
                new_profile_list.append(profile)
            else:
                update_index = old_profile_names_index.get(profile["name"], "")
                if update_index:
                    new_profile_list[update_index] = profile
        env_configuration_json["data_warehouse_configuration"]["snowflake_profiles"] = new_profile_list
        self.create_entity(env_configuration_json)


class SecretEntity(AdminEntity):
    def __init__(self, iwx_client, metadata_csv_path):
        self.iwx_client = iwx_client
        self.metadata_csv_path = metadata_csv_path

    def validate_csv_metadata_schema(self):
        df = pd.read_csv(self.metadata_csv_path)
        columns_in_csv = list(df.columns)
        columns_in_csv = [column.lower() for column in columns_in_csv]
        expected_schema = ["name", "description", "secret_store", "secret_name"]
        columns_diff = set(columns_in_csv) - set(expected_schema)
        if len(columns_diff) >= 0:
            print("CSV metadata schema matches the expected schema")
        else:
            print(
                f"Schema validation failed. Expected schema is {expected_schema} but got schema {columns_in_csv}.Exiting...")
            exit(-100)

    def get_existing_entity_id(self, entity_name):
        get_secret_details = self.iwx_client.list_secrets(params={"filter": {"name": entity_name}, "fetch_all": "true"})
        get_secret_details = get_secret_details.get("result", {}).get("response", {}).get("result", [])
        secret_id = None
        if get_secret_details:
            secret_id = get_secret_details[0].get("id", None)
        return secret_id

    def create_entity(self, configuration_body):
        secret_name = configuration_body["name"]
        secret_id = self.get_existing_entity_id(entity_name=secret_name)
        if secret_id is None:
            create_secret_response = self.iwx_client.create_secret(data=configuration_body)
            secret_id = create_secret_response.get("result", {}).get("response", {}).get('result', {}).get('id')
            if secret_id:
                return "success", create_secret_response.get("result", {}).get("response", {})
            else:
                return "failed", create_secret_response
        else:
            existing_secret_response = self.iwx_client.get_secret_details(secret_id=secret_id)
            existing_secret_response = existing_secret_response.get("result", {}).get("response", {}).get("result", {})
            existing_secret_domains = {}
            print(f"Secret Response: {existing_secret_response}")
            if existing_secret_response:
                existing_secret_domains["domains"] = existing_secret_response.get("domains", [])
                configuration_body = self.merge_dicts(configuration_body, existing_secret_domains)
            secret_updation_response = self.iwx_client.update_secret_details(secret_id=secret_id,
                                                                             data=configuration_body)
            if secret_updation_response.get('result', {}).get('status', '') == "success":
                return "success", secret_updation_response.get("result", {}).get("response", {})
            else:
                return "failed", secret_updation_response

    def update_entity_body_as_per_csv(self):
        # validate the csv metadata schema
        self.validate_csv_metadata_schema()
        with open(self.metadata_csv_path, "r") as csv_file:
            reader = csv.DictReader(csv_file)
            columns = ["secret_name", "status", "create_update_response"]
            overall_secret_status = pd.DataFrame(columns=columns)
            for row in reader:
                try:
                    secret_store_id = None
                    if row.get("secret_store", ""):
                        secret_store = row.get("secret_store", "")
                        secret_store_res = self.iwx_client.list_secret_stores(params={"filter": {"name": secret_store}})
                        secret_store_res = secret_store_res.get("result", {}).get("response", {}).get("result", [])
                        if secret_store_res:
                            secret_store_id = secret_store_res[0]["id"]
                    if secret_store_id is None:
                        raise Exception(f"Failed to find the secret store {row.get('secret_store', '')}.Exiting...")
                        # exit(-100)
                    temp = {
                        "name": row.get("name", ""),
                        "description": row.get("description", ""),
                        "secret_store": secret_store_id,
                        "secret_name": row.get("secret_name", "")
                    }
                    # 6.0 specific changes support for assigning secrets at domain
                    domain_names = row.get("domain_names", "").split(",")
                    domain_names = [domain_name.strip() for domain_name in domain_names]
                    domains = []
                    for domain_name in domain_names:
                        get_domain_details = self.iwx_client.list_domains_as_admin(
                            params={"filter": {"name": domain_name}})
                        get_domain_details = get_domain_details.get("result", {}).get("response", {}).get("result", [])
                        domain_id = None
                        if get_domain_details:
                            domain_id = get_domain_details[0].get("id", None)
                            domains.append(domain_id)
                    if domains:
                        temp["domains"] = domains

                    secret_status, secret_response = self.create_entity(temp)
                    print(f"{row.get('name', '')} :", secret_response)
                except Exception as error:
                    print(f"Failed to create/update secret '{row['name']}' : {error}")
                    secret_status = 'failed' # pragma: allowlist secret
                    secret_response = error

                secret_status_row = pd.DataFrame(
                    [{"secret_name": row['name'],
                      "status": secret_status, "create_update_response": secret_response}])
                overall_secret_status = pd.concat([overall_secret_status, secret_status_row],
                                                  ignore_index=True)
            overall_secret_status.to_csv('overall_secret_status.csv')
            return


class DomainEntity(AdminEntity):
    def __init__(self, iwx_client, metadata_csv_path):
        self.iwx_client = iwx_client
        self.metadata_csv_path = metadata_csv_path

    def validate_csv_metadata_schema(self):
        df = pd.read_csv(self.metadata_csv_path)
        columns_in_csv = list(df.columns)
        columns_in_csv = [column.lower() for column in columns_in_csv]
        expected_schema = ["name", "description", "environment_names", "accessible_sources", "users"]
        columns_diff = set(columns_in_csv) - set(expected_schema)
        if len(columns_diff) >= 0:
            print("CSV metadata schema matches the expected schema")
        else:
            print(
                f"Schema validation failed. Expected schema is {expected_schema} but got schema {columns_in_csv}.Exiting...")
            exit(-100)

    def get_existing_entity_id(self, entity_name):
        domain_details = self.iwx_client.list_domains_as_admin(params={"filter": {"name": entity_name}})
        domain_details = domain_details.get("result", {}).get("response", {}).get("result", [])
        domain_id = None
        if domain_details:
            domain_id = domain_details[0].get("id", None)
        return domain_id

    def create_entity(self, configuration_body):
        domain_name = configuration_body["name"]
        domain_id = self.get_existing_entity_id(entity_name=domain_name)
        if domain_id is None:
            create_domain_response = self.iwx_client.create_domain(config_body=configuration_body)
            print(create_domain_response)
            domain_id = create_domain_response.get("result", {}).get("response", {}).get("result", {}).get("id", None)
            add_source_to_domain_response = self.iwx_client.add_source_to_domain(domain_id=domain_id, config_body={
                "entity_ids": configuration_body.get("entity_ids", [])})
            print(add_source_to_domain_response.get("result", {}).get("response", {}))
            if domain_id:
                return "success", create_domain_response.get("result", {}).get("response", {})
            else:
                return "failed", create_domain_response
        else:
            existing_domain_response = self.iwx_client.get_domain_details(domain_id=domain_id)
            existing_domain_response = existing_domain_response.get("result", {}).get("response", {}).get("result", {})
            existing_domain_secrets = {}
            if existing_domain_response:
                existing_domain_secrets["secrets"] = existing_domain_response.get("secrets", [])
                configuration_body = self.merge_dicts(configuration_body, existing_domain_secrets)
            domain_updation_response = self.iwx_client.update_domain(domain_id=domain_id,
                                                                     config_body=configuration_body)
            already_existing_source_ids_response = self.iwx_client.get_sources_associated_with_domain(
                domain_id=domain_id)
            already_existing_source = already_existing_source_ids_response.get("result", {}).get("response", {}).get(
                "result", [])
            already_existing_source_ids = [source["id"] for source in already_existing_source]
            final_list_of_sources_to_add = copy.deepcopy(already_existing_source_ids)
            sources_to_be_added = configuration_body.get("entity_ids", [])
            for source_id in sources_to_be_added:
                if source_id not in already_existing_source_ids:
                    final_list_of_sources_to_add.append(source_id)
            print("final_list_of_sources_to_add", final_list_of_sources_to_add)
            add_source_to_domain_response = self.iwx_client.add_source_to_domain(domain_id=domain_id,
                                                                                 config_body={
                                                                                     "entity_ids": final_list_of_sources_to_add})
            print(add_source_to_domain_response.get("result", {}).get("response", {}))
            if domain_updation_response.get('result', {}).get('status', '') == "success":
                return "success", domain_updation_response.get("result", {}).get("response", {})
            else:
                return "failed", domain_updation_response

    def update_entity_body_as_per_csv(self):
        self.validate_csv_metadata_schema()
        environment_name_id_lookup = {}
        environments = self.iwx_client.get_environment_details(params={"fetch_all":True})
        environments = environments.get("result", {}).get("response", {}).get("result", [])
        for environment in environments:
            environment_name_id_lookup[environment["name"].lower()]=environment["id"]
        with open(self.metadata_csv_path, "r") as csv_file:
            reader = csv.DictReader(csv_file)
            columns = ["domain_name", "status", "create_update_response"]
            overall_domain_status = pd.DataFrame(columns=columns)
            for row in reader:
                try:
                    temp = {
                        "name": row.get("name", ""),
                        "description": row.get("description", "")
                    }
                    temp["environment_ids"]=[]
                    # if updating the domain
                    existing_domain_id = self.get_existing_entity_id(entity_name=temp["name"])
                    if existing_domain_id:
                        existing_domain_details = self.iwx_client.get_domain_details(domain_id=existing_domain_id)
                        existing_domain_details = existing_domain_details.get("result", {}).get("response", {}).get(
                            "result", {})
                        temp["environment_ids"] = existing_domain_details["environment_ids"]
                        if existing_domain_details.get("secrets", []):
                            temp["secrets"] = existing_domain_details["secrets"]
                        if existing_domain_details.get("users", []):
                            temp["users"] = existing_domain_details["users"]
                    environment_names = row.get("environment_names", "").split(",")
                    environment_ids = [
                        environment_name_id_lookup.get(environment_name.lower(),None) for environment_name in environment_names]
                    environment_ids = [env_id for env_id in environment_ids if env_id is not None]
                    temp["environment_ids"].extend(environment_ids)
                    temp["environment_ids"] = list(set(temp["environment_ids"]))
                    current_user_details = self.iwx_client.list_users(
                        params={"filter": {"refreshToken": self.iwx_client.client_config.get("refresh_token", "")}})
                    current_user_result = current_user_details.get("result", {}).get("response", {}).get("result", [])
                    if len(current_user_result) > 0:
                        current_user_id = current_user_result[0]["id"]
                        temp["users"] = [current_user_id]
                    users = row.get("users", "").split(",")
                    for user in users:
                        user_details = self.iwx_client.list_users(
                            params={"filter": {"profile.email": user}})
                        user_result = user_details.get("result", {}).get("response", {}).get("result", [])
                        for filtered_user in user_result:
                            filtered_user_id = filtered_user.get("id", "")
                            if filtered_user_id:
                                temp["users"].append(filtered_user_id)
                            else:
                                filtered_user_email = filtered_user.get("profile", {}).get("email", "")
                                print(f"Did not find user id {filtered_user_email}. Ignoring the user")
                    if list(set(temp["users"])):
                        temp["users"] = list(set(temp["users"]))
                    # 6.0 specific changes support for assigning secrets at domain
                    secrets = row.get("secret_names", "").split(",")
                    secrets = [secret.strip() for secret in secrets if len(secret.strip()) > 0]
                    temp["secrets"] = []
                    print(f"Secrets Data: {secrets}")
                    for secret in secrets:
                        secret_details = self.iwx_client.list_secrets(
                            params={"filter": {"name": secret}})
                        secret_result = secret_details.get("result", {}).get("response", {}).get("result", [])
                        print(f"Secret Result : {secret_result}")
                        if not secret_result:
                            print(f"Could not find the secret with the name {secret}. Please validate the same!")
                            raise Exception(
                                f"Could not find the secret with the name {secret}. Please validate the same!")
                        for filtered_secret in secret_result:
                            filtered_secret_id = filtered_secret.get("id", "")
                            if filtered_secret_id:
                                temp["secrets"].append(filtered_secret_id)
                            else:
                                print(f"Did not find secret {secret}. Ignoring the user")
                    temp["secrets"] = list(set(temp["secrets"]))
                    accessible_sources = row.get("accessible_sources", [])
                    accessible_sources = accessible_sources.split(",") if accessible_sources is not None else []
                    accessible_source_ids = []
                    for source_name in accessible_sources:
                        source_result = self.iwx_client.get_list_of_sources(
                            params={"filter": {"name": source_name.strip()}}).get("result", {}).get("response",
                                                                                                    {}).get("result",
                                                                                                            [])
                        if source_result:
                            source_id = source_result[0].get("id", None)
                            if source_id is not None:
                                accessible_source_ids.append(source_id)
                            else:
                                print(f"Source with the name {source_name} not found.Skipping..")
                    if len(accessible_source_ids) > 0:
                        temp["entity_ids"] = accessible_source_ids
                    saml_details = self.iwx_client.list_auth_configs()
                    saml_config = [config for config in saml_details["result"]["response"]["result"] if
                                   config['authentication_type'] == 'saml' and config["is_active"]]
                    if saml_config:
                        del temp["users"]
                    domain_status, domain_response = self.create_entity(temp)
                    print(f"{row.get('name', '')} :", domain_response)
                except Exception as error:
                    print(f"Failed to create/update domain for {row['name']}: {error}")
                    domain_status = 'failed'
                    domain_response = error

                domain_status_row = pd.DataFrame(
                    [{"domain_name": row['name'],
                      "status": domain_status, "create_update_response": domain_response}])
                overall_domain_status = pd.concat([overall_domain_status, domain_status_row],
                                                  ignore_index=True)
            overall_domain_status.to_csv('overall_domain_status.csv')
            return


class ComputeEntity(AdminEntity):
    def __init__(self, iwx_client, metadata_csv_path):
        self.environment_id = None
        self.iwx_client = iwx_client
        self.metadata_csv_path = metadata_csv_path

    def validate_csv_metadata_schema(self):
        df = pd.read_csv(self.metadata_csv_path)
        columns_in_csv = list(df.columns)
        columns_in_csv = [column.lower() for column in columns_in_csv]
        expected_schema = ["name", "environment_name", "launch_strategy", "is_default_cluster",
                           "is_interactive_cluster", "runtime_version", "driver_node_type", "worker_node_type",
                           "num_worker_nodes", "max_allowed_workers", "idle_termination_timeout"]
        columns_diff = set(columns_in_csv) - set(expected_schema)
        if len(columns_diff) >= 0:
            print("CSV metadata schema matches the expected schema")
        else:
            print(f"Schema validation failed. Expected schema is {expected_schema} but got schema {columns_in_csv}."
                  f"Exiting...")
            exit(-100)

        # Add validation for run time versions, environment type, compute type

    def get_existing_entity_id(self, entity_name):
        compute_id_response = self.iwx_client.get_compute_id_from_name(environment_id=self.environment_id,
                                                                       compute_name=entity_name)
        compute_id = compute_id_response.get('result', {}).get('response', {}).get('compute_id', None)
        return compute_id

    def create_entity(self, configuration_body):
        compute_name = configuration_body["name"]
        environment_id = configuration_body['environment_id']
        self.environment_id = environment_id
        compute_type = "interactive" if configuration_body.get("launch_strategy") == "persistent" else "ephemeral"
        configuration_body.pop('environment_id')
        compute_id = self.get_existing_entity_id(compute_name)
        if compute_id is None:
            create_compute_response = self.iwx_client.create_compute_cluster(environment_id=environment_id,
                                                                             compute_body=configuration_body,
                                                                             compute_type=compute_type)
            compute_id = create_compute_response.get("result", {}).get("response", {}).get("result", {}).get("id")
            print(f"Create Compute Response: {create_compute_response}")
            print(f"Compute Id : {compute_id}")
            if compute_id:
                return 'success', create_compute_response
            else:
                return 'failed', create_compute_response
        else:
            update_compute_response = self.iwx_client.update_compute_cluster(environment_id=environment_id,
                                                                             compute_id=compute_id,
                                                                             compute_body=configuration_body,
                                                                             compute_type=compute_type)
            print(f"Update Compute Response : {update_compute_response}")
            if update_compute_response.get('result', {}).get('status', '') == "success":
                return 'success', update_compute_response
            else:
                return 'failed', update_compute_response

    def export_status_into_csv(self, data):
        data.to_csv('overall_compute_status.csv')

    def update_entity_body_as_per_csv(self):
        runtime_label_mappings = {
            "11.3_photon_enabled": {"runtime_version": "11.3.x-photon-scala2.12",
                                    "ml_runtime_version": "11.3.x-cpu-ml-photon-scala2.12",
                                    "spark_minor_version": "3.3.x", "python": "3.9"},
            "11.3": {"runtime_version": "11.3.x-scala2.12", "ml_runtime_version": "11.3.x-cpu-ml-scala2.12",
                     "spark_minor_version": "3.3.x", "python": "3.9"},
            "12.2": {"runtime_version": "12.2.x-scala2.12", "ml_runtime_version": "12.2.x-cpu-ml-scala2.12",
                     "spark_minor_version": "3.3.x", "python": "3.9"},
            "12.2_photon_enabled": {"runtime_version": "12.2.x-photon-scala2.12", "spark_minor_version": "3.3.x",
                                    "python": "3.9"},
            "14.3": {"runtime_version": "14.3.x-scala2.12", "ml_runtime_version": "14.3.x-cpu-ml-scala2.12",
                     "spark_minor_version": "3.5.x", "python": "3.10"}
        }
        with open(self.metadata_csv_path, "r") as csv_file:
            reader = csv.DictReader(csv_file)
            columns = ["compute_name", "environment_name", "status", "create_update_response"]
            overall_compute_status = pd.DataFrame(columns=columns)
            for row in reader:
                try:
                    environment_id_response = self.iwx_client.get_environment_details(
                        params={"filter": {"name": row['environment_name']}, "limit": 500, "fetch_all": True})
                    print(json.dumps(environment_id_response))
                    if environment_id_response.get('result', {}).get('response', {}).get('result', []):
                        environment_id = \
                            environment_id_response.get('result', {}).get('response', {}).get('result', [])[0][
                                'id']
                        connection_configuration = environment_id_response['result']['response']['result'][0][
                            'connection_configuration']
                        metastore_configuration = environment_id_response['result']['response']['result'][0].get(
                            'metastore', {})
                        metastore_type = metastore_configuration.get('configuration', {}).get('mode', '')
                        if connection_configuration['token']['password_type'] == "infoworks_managed": # pragma: allowlist secret
                            connection_configuration['token'].pop('password')  # pragma: allowlist secret
                    else:
                        raise Exception(f"Unable to get environment_id for environment '{row['environment_name']}'")
                    compute_engine = "databricksInteractive" if row.get(
                        "launch_strategy") == "persistent" else "databricks"
                    default_cluster = eval(row['is_default_cluster']) if row.get('is_default_cluster') else 'False'
                    max_allowed_workers = row['max_allowed_workers'] if row.get('max_allowed_workers') else 2
                    ml_workflow_supported = row['ml_workflow_supported'] if row.get(
                        'ml_workflow_supported') else 'False'
                    num_worker_nodes = int(row['num_worker_nodes']) if row.get('num_worker_nodes') else 2
                    min_worker_nodes = int(row['min_worker_nodes']) if row.get('min_worker_nodes') else 1
                    allow_zero_workers = eval(row['allow_zero_workers']) if row.get('allow_zero_workers') else False
                    enable_autoscale = eval(row['enable_autoscale']) if row.get('enable_autoscale') else False
                    advanced_configurations = json.loads(row['advanced_configurations']) if row.get(
                        'advanced_configurations') else []
                    for config in advanced_configurations:
                        active_flag = config.get('is_active', "True")
                        if isinstance(active_flag, str):
                            if active_flag.lower() == "true":
                                active_flag = "True"
                            elif active_flag.lower() == "false":
                                active_flag = "False"
                            else:
                                active_flag = "True"
                            config['is_active'] = eval(active_flag)
                        else:
                            config['is_active'] = active_flag

                    compute_template = {
                        "name": row.get("name", ""),
                        "description": row.get("description", ""),
                        "compute_engine": compute_engine,
                        "launch_strategy": row.get("launch_strategy"),
                        "compute_configuration": {
                            "connection_configuration": connection_configuration,
                            "runtime_configuration": {
                                "engine_type": "spark",
                                "runtime_version_properties": {
                                    "label": row.get("runtime_version"),
                                    "runtime_version": runtime_label_mappings[row.get("runtime_version")].get(
                                        "runtime_version"),
                                    "ml_runtime_version": runtime_label_mappings[row.get("runtime_version")].get(
                                        "ml_runtime_version"),
                                    "scala": "2.12",
                                    "spark": "3.x",
                                    "spark_minor_version": runtime_label_mappings[row.get("runtime_version")].get(
                                        "spark_minor_version"),
                                    "python": runtime_label_mappings[row.get("runtime_version", "")].get("python"),
                                },
                                "max_allowed_workers": int(max_allowed_workers),
                                "ml_workflow_supported": eval(ml_workflow_supported),
                                "worker_node_type": row.get('worker_node_type'),
                                "driver_node_type": row.get('driver_node_type'),
                                "advanced_configurations": advanced_configurations
                            }
                        }
                    }
                    if default_cluster:
                        compute_template["is_default_cluster"] = default_cluster
                    if "photon" in row.get("runtime_version"):
                        compute_template["compute_configuration"]["runtime_configuration"][
                            "runtime_version_properties"][
                            "photon_enabled"] = True
                    if row.get("launch_strategy") == "persistent":
                        is_interactive_cluster = row['is_interactive_cluster'] if row.get(
                            'is_interactive_cluster') else 'False'
                        idle_termination_timeout = row['idle_termination_timeout'] if row.get(
                            'idle_termination_timeout') else 120
                        compute_template["interactive_job_submit_enabled"] = eval(is_interactive_cluster)
                        compute_template["compute_configuration"]["runtime_configuration"]["idle_termination_timeout"] = \
                            int(idle_termination_timeout)
                    if row.get('metastore_version'):
                        compute_template["compute_configuration"]["runtime_configuration"]['metastore_version'] = row[
                            'metastore_version']
                    if allow_zero_workers:
                        compute_template["compute_configuration"]["runtime_configuration"]["allow_zero_workers"] = True
                        compute_template["compute_configuration"]["runtime_configuration"]["max_allowed_workers"] = None
                        compute_template["compute_configuration"]["runtime_configuration"]["worker_node_type"] = None
                        compute_template["compute_configuration"]["runtime_configuration"]["num_worker_nodes"] = None
                    if enable_autoscale:
                        compute_template["compute_configuration"]["runtime_configuration"]["enable_autoscale"] = True
                        compute_template["compute_configuration"]["runtime_configuration"]["autoscale"] = {
                            "min_worker_nodes": int(min_worker_nodes),
                            "max_worker_nodes": int(num_worker_nodes),
                        }
                    else:
                        compute_template["compute_configuration"]["runtime_configuration"]["num_worker_nodes"] = int(
                            num_worker_nodes)
                    if metastore_type.lower() == "unity_catalog":
                        compute_template['compute_configuration']['runtime_configuration'][
                            'runtime_version_properties'][
                            'unity_enabled'] = True
                    else:
                        compute_template['compute_configuration']['runtime_configuration'][
                            'runtime_version_properties']['unity_enabled'] = False

                    compute_template['environment_id'] = environment_id
                    print(f"Compute Template : {json.dumps(compute_template)}")
                    compute_status, compute_response = self.create_entity(compute_template)

                except Exception as error:
                    print(f"Failed to create / update compute for '{row['name']}' : {error}")
                    compute_status = 'failed'
                    compute_response = error

                compute_status_row = pd.DataFrame(
                    [{"compute_name": row['name'], "environment_name": row['environment_name'],
                      "status": compute_status, "create_update_response": compute_response}])
                overall_compute_status = pd.concat([overall_compute_status, compute_status_row], ignore_index=True)
            self.export_status_into_csv(overall_compute_status)
            return


class StorageEntity(AdminEntity):
    def __init__(self, iwx_client, metadata_csv_path):
        self.environment_id = None
        self.iwx_client = iwx_client
        self.metadata_csv_path = metadata_csv_path

    def get_existing_entity_id(self, entity_name):
        storage_id_response = self.iwx_client.get_storage_id_from_name(environment_id=self.environment_id,
                                                                       storage_name=entity_name)
        storage_id = storage_id_response.get('result', {}).get('response', {}).get('storage_id', None)
        return storage_id

    def create_entity(self, configuration_body):
        storage_name = configuration_body["name"]
        environment_id = configuration_body['environment_id']
        self.environment_id = environment_id
        configuration_body.pop('environment_id')
        storage_id = self.get_existing_entity_id(storage_name)
        if storage_id is None:
            create_storage_response = self.iwx_client.create_storage(environment_id=self.environment_id,
                                                                     storage_body=configuration_body)

            storage_id = create_storage_response.get("result", {}).get("response", {}).get("result", {}).get("id")
            print(f"Storage Id : {storage_id}")
            print(f"Create Storage Response: {create_storage_response}")
            if storage_id:
                return 'success', create_storage_response
            else:
                return 'failed', create_storage_response
        else:
            update_storage_response = self.iwx_client.update_storage(environment_id=self.environment_id,
                                                                     storage_id=storage_id,
                                                                     storage_body=configuration_body)
            print(f"Update Storage Response: {update_storage_response}")
            if update_storage_response.get('result', {}).get('status', '') == "success":
                return 'success', update_storage_response
            else:
                return 'failed', update_storage_response

    def export_status_into_csv(self, data):
        data.to_csv('overall_storage_status.csv')

    def update_entity_body_as_per_csv(self):
        with open(self.metadata_csv_path, "r") as csv_file:
            reader = csv.DictReader(csv_file)
            columns = ["storage_name", "environment_name", "status", "create_update_response"]
            overall_storage_status = pd.DataFrame(columns=columns)
            for row in reader:
                try:
                    storage_type = row.get('storage_type', '')
                    environment_id_response = self.iwx_client.get_environment_details(
                        params={"filter": {"name": row['environment_name']}, "limit": 500, "fetch_all": True})

                    if environment_id_response.get('result', {}).get('response', {}).get('result', []):
                        environment_id = \
                            environment_id_response.get('result', {}).get('response', {}).get('result', [])[0]['id']
                    else:
                        raise Exception(f"Unable to get environment_id for environment '{row['environment_name']}'")
                    is_default_storage = eval(row['is_default_storage']) if row.get('is_default_storage') else False
                    storage_template = {
                        "name": row.get('name', ''),
                        "storage_type": row.get('storage_type', ''),
                        "storage_integration_name": "",
                        "is_default_storage": is_default_storage,
                        "storage_authentication": {},
                    }

                    if storage_type == "adls_gen2":
                        storage_info = ast.literal_eval(row.get('storage_info'))
                        authentication_mechanism = storage_info.get('authentication_mechanism', '')
                        if authentication_mechanism == "service_principal_with_service_credentials":
                            storage_authentication_json = {
                                "scheme": storage_info.get('access_scheme', ''),
                                "authentication_mechanism": "service_principal",
                                "file_system": storage_info.get('container_name', ''),
                                "storage_account_name": storage_info.get('storage_account_name', ''),
                                "application_id": storage_info.get('application_id', ''),
                                "directory_id": storage_info.get('directory_id', '')
                            }
                        elif authentication_mechanism:
                            storage_authentication_json = {
                                "scheme": storage_info.get('access_scheme', ''),
                                "authentication_mechanism": "access_key",
                                "file_system": storage_info.get('container_name', ''),
                                "storage_account_name": storage_info.get('storage_account_name', '')
                            }
                        else:
                            raise Exception(f"{storage_info['authentication_mechanism']} is not supported.")

                        if storage_info.get('password_type') == "external_secret_store":
                            secret_name = storage_info.get('secret_name', '')
                            get_secret_details_response = self.iwx_client.list_secrets(
                                params={'filter': {"name": secret_name}})
                            if get_secret_details_response['result'].get('status') == "success" and \
                                    get_secret_details_response['result'].get('response', {}).get('result', []):
                                secret_id = get_secret_details_response['result']['response']['result'][0]['id']
                            else:
                                raise Exception(f"Secret ID not found for name : {row.get('secret_name')}")

                            credentials_json = {
                                "password_type": "secret_store", # pragma: allowlist secret
                                "secret_id": secret_id
                            }
                            print(json.dumps(storage_template))
                        elif storage_info.get('password_type') == "infoworks_managed":
                            password = storage_info.get('password', '')
                            credentials_json = {
                                "password_type": "infoworks_managed", # pragma: allowlist secret
                                "password": password
                            }
                        elif storage_info.get('password_type') == "service_authentication":
                            service_name = storage_info.get('authentication_service_name', '')
                            authentication_services = self.iwx_client.list_service_authentication(
                                params={'filter': {'name': service_name}})
                            if authentication_services.get('result', {}).get('response', {}).get('result', []):
                                authentication_service = authentication_services['result']['response']['result'][0]
                                authentication_service_id = authentication_service.get('id')
                            else:
                                raise Exception(f"Authentication Service '{service_name}' not found")
                            credentials_json = {
                                "password_type": "service_authentication", # pragma: allowlist secret
                                "service_auth_id": authentication_service_id # pragma: allowlist secret
                            }
                        else:
                            raise Exception(f"Password Type '{storage_info.get('password_type', '')}' is not supported")
                    elif storage_type == "dbfs":
                        storage_authentication_json = {}
                        credentials_json = {}
                        authentication_mechanism = ''
                        storage_info = {}
                    else:
                        raise Exception(f"Storage Type '{storage_type}' is not supported")

                    storage_template["storage_authentication"] = storage_authentication_json
                    if credentials_json:
                        if authentication_mechanism == "service_principal_with_service_credentials":
                            storage_template["storage_authentication"]['service_credential'] = credentials_json
                        elif authentication_mechanism == "service_principal_with_authentication_service":
                            storage_template["storage_authentication"]['service_credential'] = credentials_json
                        elif authentication_mechanism == "access_key":
                            storage_template["storage_authentication"]['access_key_name'] = credentials_json
                    if storage_info.get('storage_credential_name'):
                        storage_template['storage_credential_name'] = storage_info.get('storage_credential_name')
                    print(f"Row : {json.dumps(storage_template)}")
                    storage_template['environment_id'] = environment_id
                    storage_status, storage_response = self.create_entity(storage_template)
                except Exception as error:
                    print(f"Failed to create / update storage for {row['name']} : {error}")
                    storage_status = 'failed'
                    storage_response = error

                storage_status_row = pd.DataFrame(
                    [{"storage_name": row['name'], "environment_name": row['environment_name'],
                      "status": storage_status, "create_update_response": storage_response}])
                overall_storage_status = pd.concat([overall_storage_status, storage_status_row], ignore_index=True)
            self.export_status_into_csv(overall_storage_status)
            return


class EnvironmentEntity(AdminEntity):
    def __init__(self, iwx_client, metadata_csv_path):
        self.iwx_client = iwx_client
        self.metadata_csv_path = metadata_csv_path

    def get_existing_entity_id(self, entity_name):
        # environment_id_response = self.iwx_client.get_environment_id_from_name(environment_name=entity_name)
        # environment_id = environment_id_response.get('result', {}).get('response', {}).get('environment_id')
        environment_id_response = self.iwx_client.get_environment_details(
            params={"filter": {"name": entity_name}, "limit": 500, "fetch_all": True})
        if environment_id_response.get('result', {}).get('response', {}).get('result', []):
            environment_id = environment_id_response.get('result', {}).get('response', {}).get('result', [])[0]['id']
        else:
            environment_id = None

        return environment_id

    def create_entity(self, configuration_body):
        environment_name = configuration_body['name']
        environment_id = self.get_existing_entity_id(environment_name)
        if environment_id is None:
            create_environment_response = self.iwx_client.create_environment(environment_body=configuration_body)
            environment_id = create_environment_response.get("result", {}).get("response", {}).get("result", {}).get(
                "id")
            print(f"Environment Id : {environment_id}")
            print(f"Create Environment Response: {create_environment_response}")
            if environment_id:
                return "success", create_environment_response
            else:
                return "failed", create_environment_response
        else:
            update_environment_response = self.iwx_client.update_environment(environment_id=environment_id,
                                                                             environment_body=configuration_body)

            if update_environment_response.get('result', {}).get('status', '') == "success":
                print(update_environment_response.get('result', {}).get('response', {}).get('message', ''))
                print(f"Update Environment Response: {update_environment_response}")
                return "success", update_environment_response
            else:
                print(f"Failed to update environment : Response {json.dumps(update_environment_response)}")
                return "failed", update_environment_response

    def export_status_into_csv(self, data):
        data.to_csv('overall_environment_status.csv')

    def update_entity_body_as_per_csv(self):
        with open(self.metadata_csv_path, "r") as csv_file:
            reader = csv.DictReader(csv_file)
            columns = ["environment_name", "status", "create_update_response"]
            overall_environment_status = pd.DataFrame(columns=columns)
            for row in reader:
                try:
                    data_env_type = row.get('data_env_type', '')
                    databricks_authentication = ast.literal_eval(row.get('databricks_authentication'))
                    databricks_authentication_type = databricks_authentication.get('authentication_type', '')
                    storage_type = row.get('type', '')
                    if data_env_type.lower() == "azure":
                        environment_template = {
                            "name": row.get('name'),
                            "description": row.get('description', ''),
                            "compute_engine": "databricks",
                            "connection_configuration": {
                                "region": row.get('region'),
                                "workspace_url": row.get('workspace_url')
                            },
                            "platform": "azure",
                            "support_gov_cloud": False
                        }
                        if storage_type.lower() == "databricks_internal":
                            metastore_json = {
                                "type": "hive",
                                "configuration": {
                                    "mode": "internal"
                                }
                            }

                        elif storage_type.lower() == "databricks_external":
                            metadata_configs = ast.literal_eval(row.get('metadata_configs'))
                            metastore_json = {
                                "type": "hive",
                                "configuration": {
                                    "mode": "external",
                                    "connection_url": metadata_configs.get('connection_url', ''),
                                    "driver_name": metadata_configs.get('connection_url', ''),
                                    "user_name": metadata_configs.get('user_name', ''),
                                    "password": {}
                                }
                            }
                            password_type = metadata_configs.get('password', {}).get('password_type', '')
                            if password_type == "infoworks_managed": # pragma: allowlist secret
                                metastore_json['configuration']['password']['password_type'] = "infoworks_managed" # pragma: allowlist secret
                                metastore_json['configuration']['password']['password'] = metadata_configs['password']. \
                                    get('password', '') # pragma: allowlist secret
                            elif password_type == "external_secret_store": # pragma: allowlist secret
                                secret_name = metadata_configs.get('password', {}).get('secret_name', '')
                                secret_res = self.iwx_client.list_secrets(params={"filter": {"name": secret_name}})
                                secret_res = secret_res.get("result", {}).get("response", {}).get("result", [])
                                if secret_res:
                                    secret_id = secret_res[0]["id"]
                                else:
                                    raise Exception(f"Secret '{secret_name}' not found")
                                metastore_json['configuration']['password']['password_type'] = "secret_store" # pragma: allowlist secret
                                metastore_json['configuration']['password']['secret_id'] = secret_id
                        elif storage_type.lower() == "databricks_unity_catalog":
                            metastore_json = {
                                "type": "hive",
                                "configuration": {
                                    "mode": "unity_catalog"
                                }
                            }
                        else:
                            raise Exception(f"Storage Type '{storage_type}' not supported")
                        environment_template['metastore'] = metastore_json
                    elif data_env_type.lower() == "snowflake":
                        environment_template = {
                            "name": row.get('name', ''),
                            "description": row.get('description', ''),
                            "platform": 'azure',
                            "compute_engine": "databricks",
                            "support_gov_cloud": False,
                            "data_warehouse_type": "snowflake",
                            "data_warehouse_configuration": {
                                "type": "snowflake",
                                "connection_url": row.get("connection_url", ""),
                                "account_name": row.get("account_name", "")
                            },
                            "connection_configuration": {
                                "region": row.get('region', ''),
                                "workspace_url": row.get('workspace_url', '')
                            }
                        }
                        if row['snowflake_profiles']:
                            profiles = json.loads(row['snowflake_profiles'])
                            environment_template["data_warehouse_configuration"]["snowflake_profiles"] = []
                            for record in profiles:
                                profile = {
                                    "name": record.get('name', "Default"),
                                    "is_default_profile": record.get("is_default_profile", False),
                                    "warehouse": record.get("warehouse", []),
                                    "authentication_properties": record.get("authentication_properties", []),
                                    "additional_params": record.get("additional_params", []),
                                    "session_params": record.get("session_params", [])
                                }
                                if profile['authentication_properties']['password']['password_type'] == "secret_store": # pragma: allowlist secret
                                    secret_name = profile['authentication_properties']['password']['secret_name']
                                    secret_res = self.iwx_client.list_secrets(params={"filter": {"name": secret_name}})
                                    secret_res = secret_res.get("result", {}).get("response", {}).get("result", [])
                                    secret_id = None
                                    if secret_res:
                                        secret_id = secret_res[0]["id"]
                                    else:
                                        raise Exception(f"secret {secret_name} not found.Exiting...")
                                    if secret_id:
                                        profile['authentication_properties']['password'].pop('secret_name')
                                        profile['authentication_properties']['password']['secret_id'] = secret_id
                                environment_template["data_warehouse_configuration"]["snowflake_profiles"].append(
                                    profile)
                        else:
                            raise Exception("Profile is required")
                    else:
                        raise Exception(f"Data Env Type '{data_env_type}' not supported")

                    # Databricks Authentication
                    if databricks_authentication_type.lower() == "infoworks_managed":
                        databricks_token = databricks_authentication.get('token', '')
                        environment_template['connection_configuration']['token'] = {
                            "password_type": "infoworks_managed", # pragma: allowlist secret
                            "password": databricks_token # pragma: allowlist secret
                        }
                    elif databricks_authentication_type.lower() == "external_secret_store":
                        secret_name = databricks_authentication.get('secret_name', '')
                        get_secret_details_response = self.iwx_client.list_secrets(
                            params={'filter': {"name": secret_name}})
                        if get_secret_details_response['result'].get('status') == "success" and \
                                get_secret_details_response['result'].get('response', {}).get('result', []):
                            secret_id = get_secret_details_response['result']['response']['result'][0]['id']
                        else:
                            secret_id = None
                            raise Exception(f"Secret ID not found for name : {row.get('secret_name')}")
                        environment_template['connection_configuration']['token'] = {
                            "password_type": "secret_store", # pragma: allowlist secret
                            "secret_id": secret_id # pragma: allowlist secret
                        }
                    elif databricks_authentication_type.lower() == "service_authentication":
                        service_name = databricks_authentication.get('authentication_service_name', '')
                        authentication_services = self.iwx_client.list_service_authentication(
                            params={'filter': {'name': service_name}})
                        if authentication_services.get('result', {}).get('response', {}).get('result', []):
                            authentication_service = authentication_services['result']['response']['result'][0]
                            authentication_service_id = authentication_service.get('id')
                        else:
                            raise Exception(f"Authentication Service '{service_name}' not found")
                        environment_template['connection_configuration']['token'] = {
                            "password_type": "service_authentication", # pragma: allowlist secret
                            "service_auth_id": authentication_service_id
                        }
                    else:
                        raise Exception(f"Databricks Authentication Type '{databricks_authentication_type}' "
                                        f"not supported")

                    print(f"Environment Template: {json.dumps(environment_template)}")
                    environment_status, environment_response = self.create_entity(environment_template)
                except Exception as error:
                    print(f"Failed to create / update environment for {row['name']} : {error}")
                    environment_status = 'failed'
                    environment_response = error

                environment_status_row = pd.DataFrame(
                    [{"environment_name": row['name'],
                      "status": environment_status, "create_update_response": environment_response}])
                overall_environment_status = pd.concat([overall_environment_status, environment_status_row],
                                                       ignore_index=True)
            self.export_status_into_csv(overall_environment_status)
            return


def main():
    parser = argparse.ArgumentParser(description="Admin entities creation parser ", add_help=True)
    parser.add_argument('--refresh_token', required=True, type=str, help='Pass the refresh token')
    parser.add_argument('--protocol', required=True, type=str, help='Protocol for the API calls. http/https')
    parser.add_argument('--host', required=True, type=str, help='Rest API Host')
    parser.add_argument('--port', required=True, type=str, help='Rest API Port')
    parser.add_argument('--metadata_csv_path', type=str, required=True,
                        help='Pass the absolute path to metadata csv')
    parser.add_argument('--admin_entity_type', type=str, required=True,
                        choices=["profile", "secret", "domain", "compute", "storage", "environment"])
    parser.add_argument('--environment_id', type=str, required=False,
                        help="Enter the environment id whose profile is to be updated")
    args = parser.parse_args()
    iwx_client = InfoworksClientSDK()
    iwx_client.initialize_client_with_defaults(args.protocol, args.host, args.port, args.refresh_token)
    admin_entity = None
    if args.admin_entity_type == "profile":
        environment_id = args.environment_id
        if environment_id is None:
            print("environment_id parameter must be passed to admin_entity_type environment.Exiting..")
            exit(-100)
        admin_entity = ProfileEntity(iwx_client, metadata_csv_path=args.metadata_csv_path,
                                     environment_id=environment_id)
    elif args.admin_entity_type == "secret":
        admin_entity = SecretEntity(iwx_client, metadata_csv_path=args.metadata_csv_path)
    elif args.admin_entity_type == "domain":
        admin_entity = DomainEntity(iwx_client, metadata_csv_path=args.metadata_csv_path)
    elif args.admin_entity_type == "compute":
        admin_entity = ComputeEntity(iwx_client, metadata_csv_path=args.metadata_csv_path)
    elif args.admin_entity_type == "storage":
        admin_entity = StorageEntity(iwx_client, metadata_csv_path=args.metadata_csv_path)
    elif args.admin_entity_type == "environment":
        admin_entity = EnvironmentEntity(iwx_client, metadata_csv_path=args.metadata_csv_path)
    else:
        print(f"Unknown admin_entity_type {args.admin_entity_type}.Exiting...")
        exit(-100)
    admin_entity.update_entity_body_as_per_csv()


if __name__ == '__main__':
    main()
