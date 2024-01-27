import argparse
import os
import configparser
import re
import pandas as pd
from getpass import getpass
import requests
import json
import sys
import urllib3
import traceback
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
script_path = os.path.dirname(os.path.realpath(__file__))
status_list = []

class CustomError(Exception):
    def __init__(self, message):
        self.message = message
        super(CustomError, self).__init__(self.message)


class DatabricksAPI:
    def __init__(self, db_account, db_token):
        self.db_token = db_token
        self.db_account = db_account

    def list_libraries(self, cluster_id):
        headers = {'Authorization': f'Bearer {self.db_token}'}
        request_url = f"https://{self.db_account}/api/2.0/libraries/cluster-status?cluster_id={cluster_id}"
        response = requests.get(request_url, headers=headers)
        resp_json = response.json()
        # print(json.dumps(resp_json,indent=4))
        return resp_json

    def uninstall_libraries(self, cluster_id, prefix=[]):
        message = ""
        if prefix == []:
            print("prefix for the databricks library to uninstall cannot be empty")
            raise ValueError("prefix for the databricks library to uninstall cannot be empty")
            exit(-100)
        list_libraries_response = self.list_libraries(cluster_id)
        cluster_libraries = []
        prefix_regex = "|".join(prefix)
        libraries_list = list_libraries_response.get("library_statuses", None)
        if libraries_list == None:
            print(json.dumps(list_libraries_response, indent=4))
            return 200, "Libraries list is empty!"
        for item in list_libraries_response["library_statuses"]:
            if item["library"].get("jar", "") != "":
                if re.match(f'^dbfs\:////.*/({prefix_regex})', item["library"].get("jar", "")):
                    cluster_libraries.append(item["library"])
            elif item["library"].get("egg", "") != "":
                if re.match(f'^dbfs\:////.*/({prefix_regex})', item["library"].get("egg", "")):
                    cluster_libraries.append(item["library"])
            elif item["library"].get("maven", "") != "":
                if re.match(f'^dbfs\:////.*/({prefix_regex})', item["library"].get("maven", "")):
                    cluster_libraries.append(item["library"])
            elif item["library"].get("pypi", "") != "":
                if re.match(f'^dbfs\:////.*/({prefix_regex})', item["library"].get("pypi", "")):
                    cluster_libraries.append(item["library"])
            elif item["library"].get("cran", "") != "":
                if re.match(f'^dbfs\:////.*/({prefix_regex})', item["library"].get("cran", "")):
                    cluster_libraries.append(item["library"])
            elif item["library"].get("whl", "") != "":
                if re.match(f'^dbfs\:////.*/({prefix_regex})', item["library"].get("whl", "")):
                    cluster_libraries.append(item["library"])
            else:
                print(f"Unknown library type found")
                print(item["library"])
        print("Libraries to uninstall:")
        for library in cluster_libraries:
            print(library)
        if cluster_libraries == []:
            print("There is nothing to uninstall for this cluster..")
            return 200, "There is nothing to uninstall for this cluster.."
        else:
            headers = {'Authorization': f'Bearer {self.db_token}'}
            data = json.dumps({"cluster_id": cluster_id, "libraries": cluster_libraries})
            request_url = f"https://{self.db_account}/api/2.0/libraries/uninstall"
            print("Uninstalling the libraries...")
            response = requests.post(request_url, headers=headers, data=data)
            if response.status_code == 200:
                print(f"Libraries Uninstalled successfully for cluster {cluster_id}!")
                message = f"Libraries Uninstalled successfully for cluster {cluster_id}!"
            else:
                print(f"Uninstalling libraries Failed for cluster {cluster_id}")
            resp_json = response.json()
            message = resp_json.get("message", "")
            print(json.dumps(resp_json, indent=4))
            return response.status_code, message

    def restart_cluster(self, cluster_id):
        message = ""
        headers = {'Authorization': f'Bearer {self.db_token}'}
        request_url = f"https://{self.db_account}/api/2.0/clusters/restart"
        print("Restarting the cluster..")
        data_json = json.dumps({"cluster_id": cluster_id})
        response = requests.post(request_url, headers=headers, data=data_json)
        resp_json = response.json()
        if response.status_code == 200:
            print("Cluster restarted successfully!")
            message = "Cluster restarted successfully!"
        else:
            print("Cluster restart failed!")
        print(json.dumps(resp_json, indent=4))
        return response.status_code, resp_json.get("message", message)


class InfoworksAPI:
    def __init__(self, protocol, host, port, refresh_token):
        self.protocol = protocol
        self.host = host
        self.port = port
        self.refresh_token = refresh_token
        self.bearer_token = None

    def token_valid(self):
        url = "{protocol}://{ip}:{port}/v3/security/token/validate".format(protocol=self.protocol, ip=self.host,
                                                                           port=self.port)
        headers = {
            'Authorization': 'Bearer ' + self.bearer_token,
            'Content-Type': 'application/json'
        }
        response = requests.request("GET", url, headers=headers, verify=False)
        validity_status = response.json().get("result", {}).get("is_valid", "")
        return validity_status

    def refresh_delegation_token(self):
        url = "{protocol}://{ip}:{port}/v3/security/token/access/".format(protocol=self.protocol, ip=self.host,
                                                                          port=self.port)
        headers = {
            'Authorization': 'Basic ' + self.refresh_token,
            'Content-Type': 'application/json'
        }
        response = requests.request("GET", url, headers=headers, verify=False)
        self.bearer_token = response.json().get("result").get("authentication_token")

    def call_url(self, url):
        if (self.bearer_token == None or not self.token_valid()):
            self.refresh_delegation_token()
        headers = {
            'Authorization': f'Bearer {self.bearer_token}',
            'Content-Type': 'application/json'
        }
        try:
            print("url {url}".format(url=url))
            response = requests.request("GET", url, headers=headers, verify=False)
            if response is not None:
                return response
        except:
            raise CustomError("Unable to get response for url: {url}".format(url=url))

    def get_environments_list(self):
        if (self.bearer_token == None or not self.token_valid()):
            self.refresh_delegation_token()
        headers = {
            'Authorization': f'Bearer {self.bearer_token}',
            'Content-Type': 'application/json'
        }
        get_environment_list_url = "{protocol}://{ip}:{port}/v3/admin/environment".format(protocol=self.protocol,
                                                                                          ip=self.host, port=self.port)
        environment_ids = []
        print(f"Environment list url : {get_environment_list_url}")
        try:
            response = requests.request('GET', get_environment_list_url, headers=headers, verify=False)
            if response.status_code != 200:
                print(response.text)
                return None
            else:
                result = response.json().get("result", [])
                while (len(result) > 0):
                    environment_ids.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.json().get('links')['next'],
                                                                      protocol=self.protocol,
                                                                      ip=self.host,
                                                                      port=self.port)
                    response = self.call_url(nextUrl)
                    result = response.json().get("result", [])
                # result=response.json().get("result",[])
                environment_ids = [env_id["id"] for env_id in environment_ids]
                return environment_ids
        except Exception as e:
            traceback.print_exc()
            print(str(e))
            sys.exit(-100)

    def get_environment_details(self, environment_id):
        if (self.bearer_token == None or not self.token_valid()):
            self.refresh_delegation_token()
        get_environment_list_url = "{protocol}://{ip}:{port}/v3/admin/environment/{environment_id}?filter={\"platform\":{\"$in\":[\"azure\",\"aws\"]}}".format(
            protocol=self.protocol,
            ip=self.host, port=self.port, environment_id=environment_id)
        environment_details_response = self.call_url(get_environment_list_url)
        env_details = environment_details_response.json()["result"]
        return env_details

    def get_interactive_clusters_list(self, environment_id):
        if (self.bearer_token == None or not self.token_valid()):
            self.refresh_delegation_token()
        get_interactive_compute_list_url = "{protocol}://{ip}:{port}/v3/admin/environment/{environment_id}/environment-interactive-clusters".format(
            protocol=self.protocol,
            ip=self.host, port=self.port, environment_id=environment_id)
        environment_details_response = self.call_url(get_interactive_compute_list_url)
        interactive_compute_list = environment_details_response.json().get("result", [])
        return interactive_compute_list


class MicrosoftAPI:
    def __init__(self, tenant_id, resource_id, client_id, client_secret):
        self.tenant_id = tenant_id
        self.resource_id = resource_id
        self.client_id = client_id
        self.client_secret = client_secret

    def get_access_token(self):
        base_url = 'https://login.microsoftonline.com/%s/oauth2/token' % self.tenant_id
        return requests.post(
            base_url,
            headers={"Content-type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "resource": self.resource_id
            }
        )


def main():
    try:
        config_parser = configparser.ConfigParser()
        config_parser.read("./config.ini")
        parser = argparse.ArgumentParser(description='Uninstall databricks libraries utility')
        parser.add_argument('--auth_type', required=True, type=str,
                            help='Pass the authentication type <service_principal/db_token>',
                            choices=["service_principal", "db_token"])
        parser.add_argument('--env_ids', required=False, type=str, help='Pass the list of env ids comma separated',
                            default=[])
        args = parser.parse_args()
        env_ids = args.env_ids
        env_id_list = env_ids.split(",")
        resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"  # do not change this value
        db_token = None
        if args.auth_type == "service_principal":
            if "" not in [config_parser["service_principal"]["tenant_id"],
                          config_parser["service_principal"]["client_id"]]:
                secret_id = getpass("Enter the client secret: ", stream=None)
                microsoft_api_client = MicrosoftAPI(config_parser["service_principal"]["tenant_id"], resource_id,
                                                    config_parser["service_principal"]["client_id"], secret_id)
                login_response = microsoft_api_client.get_access_token()
                if login_response.status_code == 200:
                    db_token = login_response.json()["access_token"]
                else:
                    print("Microsoft login failed.Exiting..")
                    print(login_response.json())
                    exit(-100)
            else:
                print("subscription_id or tenant_id or client_id cannot be empty under config.ini.Exiting..")
                exit(-100)
        elif args.auth_type == "db_token":
            db_token = getpass("Enter the databricks token: ", stream=None)
        else:
            print("auth_type argument can only have ['service_principal','db_token'] values")
            raise ValueError("auth_type argument can only have ['service_principal','db_token'] values")
            exit(-100)
        db_account = config_parser["databricks_details"]["db_account_url"]
        db_account = db_account.lstrip("https://")
        if db_account == "":
            print("Databricks account url cannot be empty.Exiting..")
            raise ValueError("Databricks account url cannot be empty.Exiting..")
            exit(-100)
        protocol = config_parser["iwx_details"]["protocol"]
        host = config_parser["iwx_details"]["host"]
        port = config_parser["iwx_details"]["port"]
        refresh_token = config_parser["iwx_details"]["refresh_token"]
        prefix = config_parser["iwx_details"]["prefix"]
        infoworks_client = InfoworksAPI(protocol, host, port, refresh_token)
        if env_id_list == []:
            env_id_list = infoworks_client.get_environments_list()
        for env in env_id_list:
            databricks_api_client = DatabricksAPI(db_account, db_token)
            interactive_cluster_list = infoworks_client.get_interactive_clusters_list(env)
            for interactive_cluster in interactive_cluster_list:
                cluster_id = interactive_cluster.get("cluster_id", "")
                print("Environment ID in Infoworks : ", env)
                print("Compute Name in Infoworks : ", interactive_cluster["name"])
                print("Cluster ID : ", cluster_id)
                databricks_api_client.list_libraries(cluster_id)
                uninstall_status_code, uninstall_message = databricks_api_client.uninstall_libraries(cluster_id,
                                                                                                     prefix=prefix.split(
                                                                                                         ","))
                restart_status_code, restart_message = databricks_api_client.restart_cluster(cluster_id)
                status_list.append(
                    {"environment_id": env,
                     "compute_name": interactive_cluster["name"],
                     "cluster_id": cluster_id,
                     "library_uninstall_status": uninstall_status_code,
                     "library_uninstall_message": uninstall_message,
                     "cluster_restart_status": restart_status_code,
                     "cluster_restart_message": restart_message
                     })
        res_df = pd.DataFrame(status_list)
        res_df.to_csv(f"{script_path}/uninstall_library_output.csv", index=False)
        print(res_df)
        print(
            f"please find the cluster libraries uninstall status for each cluster here: {script_path}/uninstall_library_output.csv")
    except Exception as e:
        print("Execution Failed! More details..")
        print(str(e))
        print(traceback.format_exc())

if __name__ == '__main__':
    main()