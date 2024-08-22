#Usage:
#python create_admin_entities_in_bulk.py --protocol <http/https> --host <hostname> --port <443/3000> --refresh_token <Infoworks admin user refresh token> --metadata_csv_path <path to metadata csv> --admin_entity_type <environment/secret/domain>
import argparse
import os
import subprocess
import sys
import pandas as pd
import copy
import traceback
from abc import ABC, abstractmethod
import json
import csv
import pkg_resources
cwd=os.path.dirname(os.path.realpath(__file__))
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.path.insert(0,"/Users/nitin.bs/PycharmProjects/infoworks-python-sdk/")
# required = {'infoworkssdk==5.0.3'}
# installed = {pkg.key for pkg in pkg_resources.working_set}
# missing = required - installed
# if missing:
#     python = sys.executable
#     subprocess.check_call([sys.executable, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
from infoworks.sdk.client import InfoworksClientSDK

class AdminEntity(ABC):

    def merge_dicts(self,dict1, dict2):
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
    def get_existing_entity_id(self,entity_name):
        pass

    @abstractmethod
    def create_entity(self,configuration_body):
        pass

    @abstractmethod
    def update_entity_body_as_per_csv(self):
        pass



class EnvironmentEntity(AdminEntity):
    def __init__(self,iwx_client,metadata_csv_path,environment_id):
        self.iwx_client = iwx_client
        self.metadata_csv_path = metadata_csv_path
        self.environment_id = environment_id

    def validate_csv_metadata_schema(self):
        df = pd.read_csv(self.metadata_csv_path)
        columns_in_csv=list(df.columns)
        columns_in_csv=[column.lower() for column in columns_in_csv]
        expected_schema = ["name","is_default_profile","warehouse","username","secretname","additional_params","session_params"]
        columns_diff = set(columns_in_csv)-set(expected_schema)
        if len(columns_diff)>=0:
            print("CSV metadata schema matches the expected schema")
        else:
            print(f"Schema validation failed. Expected schema is {expected_schema} but got schema {columns_in_csv}.Exiting...")
            exit(-100)

    def get_existing_entity_id(self,entity_name):
        get_environment_details = self.iwx_client.get_environment_id_from_name(environment_name=entity_name)
        environment_id = get_environment_details.get("result",{}).get("response",{}).get("environment_id",None)
        return environment_id

    def create_entity(self,configuration_body):
        environment_id = self.environment_id
        if environment_id is None:
            env_creation_response = self.iwx_client.create_environment(configuration_body)
            print(env_creation_response.get("result",{}).get("response",{}))
        else:
            env_updation_response = self.iwx_client.update_environment(environment_id=environment_id,environment_body=configuration_body)
            print(env_updation_response.get("result",{}).get("response",{}))

    def update_entity_body_as_per_csv(self):
        self.validate_csv_metadata_schema()
        with open(self.metadata_csv_path, "r") as csv_file:
            reader = csv.DictReader(csv_file)
            snowflake_profile = []
            for row in reader:
                temp={
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
                if row.get("name",""):
                    temp["name"]=row.get("name","")
                if row.get("is_default_profile", ""):
                    temp["is_default_profile"] = json.loads(row.get("is_default_profile", "false"))
                if row.get("warehouse",[]):
                    temp["warehouse"]=json.loads(row.get("warehouse","[]"))
                if row.get("additional_params",[]):
                    temp["additional_params"] = json.loads(row.get("additional_params", "[]"))
                if row.get("session_params",[]):
                    temp["session_params"] = json.loads(row.get("session_params", "[]"))
                if row.get("username",""):
                    temp["authentication_properties"]["username"]=row.get("username","")
                if row.get("secretname",""):
                    secret_name = row.get("secretname","")
                    secret_res = self.iwx_client.list_secrets(params={"filter":{"name":secret_name}})
                    secret_res = secret_res.get("result",{}).get("response",{}).get("result",[])
                    secret_id=None
                    if secret_res:
                        secret_id = secret_res[0]["id"]
                    else:
                        print(f"secret {secret_name} not found.Exiting...")
                        exit(-100)
                    if secret_id:
                        temp["authentication_properties"]["password"]["password_type"]="secret_store" #pragma: allowlist secret
                        temp["authentication_properties"]["password"]["secret_id"]=secret_id
                snowflake_profile.append(temp)
        env_configuration_response = self.iwx_client.get_environment_details(environment_id=self.environment_id)
        env_configuration_json = env_configuration_response.get("result",{}).get("response",{}).get("result",{})
        if env_configuration_json:
            env_configuration_json=env_configuration_json[0]
        else:
            print("Failed while getting exiting env details")
            print(env_configuration_response)
        old_profile_list = env_configuration_json.get("data_warehouse_configuration",{}).get("snowflake_profiles",[])
        old_profile_names_index = {}
        old_profile_names =[]
        for i,old_profile in enumerate(old_profile_list):
            old_profile_names_index[old_profile["name"]] = i
            old_profile_names.append(old_profile["name"])
        new_profile_list = copy.deepcopy(old_profile_list)
        for i,profile in enumerate(snowflake_profile):
            if profile["name"] not in old_profile_names:
                new_profile_list.append(profile)
            else:
                update_index = old_profile_names_index.get( profile["name"],"")
                if update_index:
                    new_profile_list[update_index] = profile
        env_configuration_json["data_warehouse_configuration"]["snowflake_profiles"]=new_profile_list
        self.create_entity(env_configuration_json)

class SecretEntity(AdminEntity):
    def __init__(self,iwx_client,metadata_csv_path):
        self.iwx_client = iwx_client
        self.metadata_csv_path = metadata_csv_path

    def validate_csv_metadata_schema(self):
        df = pd.read_csv(self.metadata_csv_path)
        columns_in_csv=list(df.columns)
        columns_in_csv=[column.lower() for column in columns_in_csv]
        expected_schema = ["name","description","secret_store","secret_name"]
        columns_diff = set(columns_in_csv)-set(expected_schema)
        if len(columns_diff)>=0:
            print("CSV metadata schema matches the expected schema")
        else:
            print(f"Schema validation failed. Expected schema is {expected_schema} but got schema {columns_in_csv}.Exiting...")
            exit(-100)

    def get_existing_entity_id(self,entity_name):
        get_secret_details = self.iwx_client.list_secrets(params={"filter":{"name": entity_name},"fetch_all":"true"})
        get_secret_details = get_secret_details.get("result",{}).get("response",{}).get("result",[])
        secret_id = None
        if get_secret_details:
            secret_id=get_secret_details[0].get("id",None)
        return secret_id


    def create_entity(self,configuration_body):
        secret_name = configuration_body["name"]
        secret_id = self.get_existing_entity_id(entity_name=secret_name)
        if secret_id is None:
            create_secret_response = self.iwx_client.create_secret(data=configuration_body)
            return create_secret_response.get("result",{}).get("response",{})
        else:
            existing_secret_response = self.iwx_client.get_secret_details(secret_id=secret_id)
            existing_secret_response = existing_secret_response.get("result",{}).get("response",{}).get("result",{})
            existing_secret_domains = {}
            if existing_secret_response:
                existing_secret_domains["domains"] = existing_secret_response.get("domains",[])
                configuration_body = self.merge_dicts(configuration_body,existing_secret_domains)
            secret_updation_response = self.iwx_client.update_secret_details(secret_id=secret_id,data=configuration_body)
            return secret_updation_response.get("result",{}).get("response",{})

    def update_entity_body_as_per_csv(self):
        #validate the csv metadata schema
        self.validate_csv_metadata_schema()
        with open(self.metadata_csv_path, "r") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                secret_store_id = None
                if row.get("secret_store", ""):
                    secret_store = row.get("secret_store", "")
                    secret_store_res = self.iwx_client.list_secret_stores(params={"filter": {"name": secret_store}})
                    secret_store_res = secret_store_res.get("result", {}).get("response", {}).get("result", [])
                    if secret_store_res:
                        secret_store_id = secret_store_res[0]["id"]
                if secret_store_id is None:
                    print(f"Failed to find the secret store {row.get('secret_store', '')}.Exiting...")
                    exit(-100)
                temp={
                "name": row.get("name",""),
                "description": row.get("description",""),
                "secret_store": secret_store_id,
                "secret_name": row.get("secret_name","")
                }
                # 6.0 specific changes support for assigning secrets at domain
                domain_names = row.get("domain_names", "").split(",")
                domain_names = [domain_name.strip() for domain_name in domain_names]
                domains = []
                for domain_name in domain_names:
                    get_domain_details = self.iwx_client.list_domains_as_admin(params={"filter": {"name": domain_name}})
                    get_domain_details = get_domain_details.get("result", {}).get("response", {}).get("result", [])
                    domain_id = None
                    if get_domain_details:
                        domain_id = get_domain_details[0].get("id", None)
                        domains.append(domain_id)
                if domains:
                    temp["domains"] = domains
                create_secret_response = self.create_entity(temp)
                print(f"{row.get('name','')} :",create_secret_response)

            return

class DomainEntity(AdminEntity):
    def __init__(self,iwx_client,metadata_csv_path):
        self.iwx_client = iwx_client
        self.metadata_csv_path = metadata_csv_path

    def validate_csv_metadata_schema(self):
        df = pd.read_csv(self.metadata_csv_path)
        columns_in_csv=list(df.columns)
        columns_in_csv=[column.lower() for column in columns_in_csv]
        expected_schema = ["name","description","environment_names","accessible_sources","users"]
        columns_diff = set(columns_in_csv)-set(expected_schema)
        if len(columns_diff)>=0:
            print("CSV metadata schema matches the expected schema")
        else:
            print(f"Schema validation failed. Expected schema is {expected_schema} but got schema {columns_in_csv}.Exiting...")
            exit(-100)

    def get_existing_entity_id(self,entity_name):
        domain_details = self.iwx_client.list_domains(params={"filter":{"name":entity_name}})
        domain_details = domain_details.get("result",{}).get("response",{}).get("result",[])
        domain_id = None
        if domain_details:
            domain_id=domain_details[0].get("id",None)
        return domain_id

    def create_entity(self,configuration_body):
        domain_name = configuration_body["name"]
        domain_id = self.get_existing_entity_id(entity_name=domain_name)
        if domain_id is None:
            create_domain_response = self.iwx_client.create_domain(config_body=configuration_body)
            print(create_domain_response)
            domain_id = create_domain_response.get("result",{}).get("response",{}).get("result",{}).get("id",None)
            add_source_to_domain_response = self.iwx_client.add_source_to_domain(domain_id= domain_id,config_body={"entity_ids":configuration_body.get("entity_ids",[])})
            print(add_source_to_domain_response.get("result", {}).get("response", {}))
            return create_domain_response.get("result",{}).get("response",{})
        else:
            existing_domain_response = self.iwx_client.get_domain_details(domain_id=domain_id)
            existing_domain_response = existing_domain_response.get("result", {}).get("response", {}).get("result", {})
            existing_domain_secrets = {}
            if existing_domain_response:
                existing_domain_secrets["secrets"] = existing_domain_secrets.get("secrets", [])
                configuration_body = self.merge_dicts(configuration_body, existing_domain_secrets)
            domain_updation_response = self.iwx_client.update_domain(domain_id=domain_id,config_body=configuration_body)
            already_existing_source_ids_response = self.iwx_client.get_sources_associated_with_domain(domain_id=domain_id)
            already_existing_source=already_existing_source_ids_response.get("result",{}).get("response",{}).get("result",[])
            already_existing_source_ids = [source["id"] for source in already_existing_source]
            final_list_of_sources_to_add = copy.deepcopy(already_existing_source_ids)
            sources_to_be_added = configuration_body.get("entity_ids", [])
            for source_id in sources_to_be_added:
                if source_id not in already_existing_source_ids:
                    final_list_of_sources_to_add.append(source_id)
            print("final_list_of_sources_to_add",final_list_of_sources_to_add)
            add_source_to_domain_response = self.iwx_client.add_source_to_domain(domain_id=domain_id,
                                                 config_body={"entity_ids": final_list_of_sources_to_add})
            print(add_source_to_domain_response.get("result",{}).get("response",{}))
            return domain_updation_response.get("result",{}).get("response",{})

    def update_entity_body_as_per_csv(self):
        self.validate_csv_metadata_schema()
        with open(self.metadata_csv_path, "r") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                temp={
                "name": row.get("name",""),
                "description": row.get("description","")
                }
                # if updating the domain
                existing_domain_id = self.get_existing_entity_id(entity_name=temp["name"])
                if existing_domain_id:
                    existing_domain_details = self.iwx_client.get_domain_details(domain_id=existing_domain_id)
                    existing_domain_details = existing_domain_details.get("result",{}).get("response",{}).get("result",{})
                    temp["environment_ids"] = existing_domain_details["environment_ids"]
                    if existing_domain_details.get("secrets",[]):
                        temp["secrets"] = existing_domain_details["secrets"]
                    if existing_domain_details.get("users",[]):
                        temp["users"] = existing_domain_details["users"]
                environment_names = row.get("environment_names","").split(",")
                environment_ids = [self.iwx_client.get_environment_id_from_name(environment_name).get("result",{}).get("response",{}).get("environment_id",None) for environment_name in environment_names]
                environment_ids = [env_id for env_id in environment_ids if env_id is not None]
                temp["environment_ids"]=environment_ids
                current_user_details = self.iwx_client.list_users(params={"filter":{"refreshToken":self.iwx_client.client_config.get("refresh_token","")}})
                current_user_result = current_user_details.get("result",{}).get("response",{}).get("result",[])
                if len(current_user_result)>0:
                    current_user_id=current_user_result[0]["id"]
                    temp["users"]=[current_user_id]
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
                temp["users"] = list(set(temp["users"]))
                # 6.0 specific changes support for assigning secrets at domain
                secrets = row.get("secret_names", "").split(",")
                secrets = [secret.strip() for secret in secrets]
                temp["secrets"] = []
                for secret in secrets:
                    secret_details = self.iwx_client.list_secrets(
                        params={"filter": {"name": secret}})
                    secret_result = secret_details.get("result", {}).get("response", {}).get("result", [])
                    for filtered_secret in secret_result:
                        filtered_secret_id = filtered_secret.get("id", "")
                        if filtered_secret_id:
                            temp["secrets"].append(filtered_secret_id)
                        else:
                            print(f"Did not find secret {secret}. Ignoring the user")
                temp["secrets"] = list(set(temp["secrets"]))
                accessible_sources = row.get("accessible_sources", [])
                accessible_sources=accessible_sources.split(",") if accessible_sources is not None else []
                accessible_source_ids = []
                for source_name in accessible_sources:
                    source_result = self.iwx_client.get_list_of_sources(params={"filter":{"name":source_name.strip()}}).get("result", {}).get("response",
                                                                                                          {}).get("result",[])
                    if source_result:
                        source_id = source_result[0].get("id",None)
                        if source_id is not None:
                            accessible_source_ids.append(source_id)
                        else:
                            print(f"Source with the name {source_name} not found.Skipping..")
                if len(accessible_source_ids) > 0:
                    temp["entity_ids"] = accessible_source_ids
                create_domain_response = self.create_entity(temp)
                print(f"{row.get('name','')} :",create_domain_response)
            return

def main():
    parser = argparse.ArgumentParser(description="Admin entities creation parser ",add_help=True)
    parser.add_argument('--refresh_token', required=True, type=str, help='Pass the refresh token')
    parser.add_argument('--protocol', required=True, type=str, help='Protocol for the API calls. http/https')
    parser.add_argument('--host', required=True, type=str, help='Rest API Host')
    parser.add_argument('--port', required=True, type=str, help='Rest API Port')
    parser.add_argument('--metadata_csv_path', type=str, required=True,
                        help='Pass the absolute path to metadata csv')
    parser.add_argument('--admin_entity_type', type=str, required=True,
                        choices=["environment","secret","domain"])
    parser.add_argument('--environment_id', type=str, required=False,help = "Enter the environment id whose profile is to be updated")
    args = parser.parse_args()
    iwx_client = InfoworksClientSDK()
    iwx_client.initialize_client_with_defaults(args.protocol, args.host, args.port, args.refresh_token)
    admin_entity=None
    if args.admin_entity_type == "environment":
        environment_id = args.environment_id
        if environment_id is None:
            print("environment_id parameter must be passed to admin_entity_type environment.Exiting..")
            exit(-100)
        admin_entity=EnvironmentEntity(iwx_client,metadata_csv_path=args.metadata_csv_path,environment_id=environment_id)
    elif args.admin_entity_type == "secret":
        admin_entity = SecretEntity(iwx_client, metadata_csv_path=args.metadata_csv_path)
    elif args.admin_entity_type == "domain":
        admin_entity = DomainEntity(iwx_client, metadata_csv_path=args.metadata_csv_path)
    else:
        print(f"Unknown admin_entity_type {args.admin_entity_type}.Exiting...")
        exit(-100)
    admin_entity.update_entity_body_as_per_csv()

if __name__ == '__main__':
    main()