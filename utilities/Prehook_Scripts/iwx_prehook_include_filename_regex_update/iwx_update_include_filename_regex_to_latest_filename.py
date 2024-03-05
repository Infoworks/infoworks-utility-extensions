import sys
import requests
import warnings
from datetime import datetime,timedelta
import configparser
import os
import argparse
import subprocess
warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
warnings.filterwarnings("ignore")
required = {'infoworkssdk==4.0a13'}
import pkg_resources
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
from infoworks.sdk.client import InfoworksClientSDK

def main():
    parser = argparse.ArgumentParser(description='Prehook to update table regex')
    parser.add_argument('--private_key_file', required=True, type=str, help='Pass the name of private key file')
    args = parser.parse_args()
    job_type = os.environ.get('jobType')
    if job_type in ("source_structured_crawl", "source_structured_cdc_merge"):
        print("Executing pre hook script")
    else:
        print("Skipping the pre hook since the jobType is not source_structured_crawl or cdc_merge")
        sys.exit(0)
    #Get the path of the currently executing Python script
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
    table_name = os.environ.get('tableNameAtSource')
    private_key_file = args.private_key_file
    config = configparser.ConfigParser()
    config.read(config_file_path)

    # Info works Client SDK Initialization
    host = config.get("INFOWORKS_ENVIRONMENT", "HOST")
    port = config.getint("INFOWORKS_ENVIRONMENT", "PORT")
    protocol = config.get("INFOWORKS_ENVIRONMENT", "PROTOCOL")
    refresh_token = config.get("INFOWORKS_ENVIRONMENT", "REFRESH_TOKEN")
    iwx_client = InfoworksClientSDK()
    iwx_client.initialize_client_with_defaults(protocol, host, port, refresh_token)
    try:
        #code to get latest file name
        source_connection_details = iwx_client.get_source_connection_details(source_id=source_id)
        source_connection_details = source_connection_details.get("result",{}).get("response",{}).get("result",{})
        print("source_connection_details:", source_connection_details)
        table_details = iwx_client.get_table_configurations(source_id=source_id,table_id=table_id)
        table_details = table_details.get("result",{}).get("response",{}).get("result",{})
        max_modified_timestamp = table_details.get("max_modified_timestamp","")
        # SFTP_USERNAME = "infoworks"
        # SFTP_SERVER = "10.38.32.6"
        # SFTP_PORT = "22"
        # REMOTE_DIR = "/home/infoworks/source_files/"
        PRIVATE_KEY_PATH=os.path.join(script_directory,private_key_file)
        SFTP_USERNAME = source_connection_details.get("storage",{}).get("username")
        SFTP_SERVER = source_connection_details.get("storage",{}).get("sftp_host")
        SFTP_PORT = str(source_connection_details.get("storage",{}).get("sftp_port"))
        source_base_path = source_connection_details.get("source_base_path_relative","")
        table_relative_path = table_details.get("configuration",{}).get("source_relative_path","/")
        table_relative_path = table_relative_path.lstrip("/")
        REMOTE_DIR = os.path.join(source_base_path,table_relative_path)
        # SSH command to get the latest file name
        s = '400'
        os.chmod(PRIVATE_KEY_PATH, int(s, base=8))
        #ssh_command = f'ssh -o StrictHostKeyChecking=no -i {PRIVATE_KEY_PATH} -p {SFTP_PORT} {SFTP_USERNAME}@{SFTP_SERVER} "cd {REMOTE_DIR} && ls -t | head -n 1"'
        if max_modified_timestamp:
            datetime_object = datetime.strptime(max_modified_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
            datetime_object = datetime_object + timedelta(seconds=1)
            formatted_timestamp = datetime_object.strftime('%Y-%m-%d %H:%M:%S')
            find_command = f"""find {REMOTE_DIR} -type f -newermt "{formatted_timestamp}" -printf "%T+ %p\\n" | sort -nr | cut -d " " -f 2 |head -n 1 |xargs -0 -n 1 basename"""
            ssh_command = f"ssh -o StrictHostKeyChecking=no -i {PRIVATE_KEY_PATH} -p {SFTP_PORT} {SFTP_USERNAME}@{SFTP_SERVER} 'cd {REMOTE_DIR} && {find_command}'"
            print("ssh_command:",ssh_command)
        else:
            ssh_command = f'ssh -o StrictHostKeyChecking=no -i {PRIVATE_KEY_PATH} -p {SFTP_PORT} {SFTP_USERNAME}@{SFTP_SERVER} "cd {REMOTE_DIR} && ls -t | head -n 1"'
        latest_file = subprocess.check_output(ssh_command, shell=True, encoding='utf-8').strip()
        print("Latest file:", latest_file)
        if latest_file:
            request_body = {
                "configuration":{
                    "include_filename_regex": '.*'+ latest_file + '.*'
                }
            }
            update_configuration_endpoint = f"{protocol}://{host}:{port}/v3/sources/{source_id}/tables/{table_id}"
            bearer_token=iwx_client.client_config.get("bearer_token")
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
            print("No most recent file found since the last execution.")
            print("Aborting...")
            exit(-100)
        # try:
        #     response = iwx_client.update_table_configurations(source_id=source_id,table_id=table_id,config_body=request_body)
        #     print(response.get("result", {}).get("response", {}))
        #     if response.get("result",{}).get("status","").upper() == "SUCCESS":
        #         print("Table configuration updated successfully")
        #     else:
        #         raise Exception(f"Failed to update table configuration - Status code: {response}")
        # except requests.RequestException as e:
        #     raise Exception("Failed to send request to update table configuration:", e)

    except requests.RequestException as e:
        raise Exception("Failed to send request to authenticate:", e)


if __name__ == '__main__':
    main()
