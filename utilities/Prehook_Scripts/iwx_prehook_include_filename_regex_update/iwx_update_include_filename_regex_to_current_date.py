import sys
import requests
import warnings
from datetime import datetime
import configparser
import os

warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
warnings.filterwarnings("ignore")

if __name__ == "__main__":
    job_type = os.environ.get('jobType')
    if job_type in ("source_structured_crawl", "source_structured_cdc_merge"):
        print("Executing pre hook script")
    else:
        print("Skipping the pre hook since the jobType is not source_structured_crawl or cdc_merge")
        sys.exit(0)
    # Get the path of the currently executing Python script
    current_script_path = os.path.abspath(__file__)

    # Get the directory containing the script
    script_directory = os.path.dirname(current_script_path)
    config_file_path = script_directory + '/config.ini'

    if not os.path.exists(config_file_path):
        raise Exception(f"{config_file_path} not found")
    else:
        print(f"Configuration File Path: {config_file_path}")

    source_id = os.environ.get('sourceId')
    table_id = os.environ.get('tableId')
    config = configparser.ConfigParser()
    config.read(config_file_path)

    # Info works Client SDK Initialization
    host = config.get("INFOWORKS_ENVIRONMENT", "HOST")
    port = config.getint("INFOWORKS_ENVIRONMENT", "PORT")
    protocol = config.get("INFOWORKS_ENVIRONMENT", "PROTOCOL")
    refresh_token = config.get("INFOWORKS_ENVIRONMENT", "REFRESH_TOKEN")

    authentication_endpoint = f"{protocol}://{host}:{port}/v3/security/token/access"
    auth_headers = {
        "Authorization": f"Basic {refresh_token}"
    }

    try:
        auth_response = requests.get(authentication_endpoint, headers=auth_headers, verify=False)
        if auth_response.status_code == 200:
            print("Authentication Successful")
            bearer_token = auth_response.json()['result']['authentication_token']

            update_configuration_endpoint = f"{protocol}://{host}:{port}/v3/sources/{source_id}/tables/{table_id}"
            request_body = {
                "configuration": {
                    "include_filename_regex": '.*'+str(datetime.now().date()) + '.*'
                }
            }
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {bearer_token}"
            }
            try:
                response = requests.patch(update_configuration_endpoint, json=request_body, headers=headers,
                                          verify=False)
                if response.status_code == 200:
                    print("Table configuration updated successfully")
                else:
                    raise Exception(f"Failed to update table configuration - Status code: {response.status_code}")
            except requests.RequestException as e:
                raise Exception("Failed to send request to update table configuration:", e)
        else:
            raise Exception(f"Failed to authenticate - Status code: {auth_response.status_code}")
    except requests.RequestException as e:
        raise Exception("Failed to send request to authenticate:", e)
