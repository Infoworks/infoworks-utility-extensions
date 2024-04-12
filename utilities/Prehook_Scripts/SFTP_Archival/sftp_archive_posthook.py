import os
import sys
import json
import logging
import datetime
import subprocess
import pkg_resources

global logger

# Installing Required Modules
required = {'infoworkssdk==4.0a14', 'paramiko'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    python = sys.executable
    subprocess.check_call(
        [sys.executable, '-m', 'pip', 'install', *missing, '--user', '--trusted-host', 'pypi.org', '--trusted-host',
         'files.pythonhosted.org'], stdout=subprocess.DEVNULL)
    user_site = subprocess.run([python, "-m", "site", "--user-site"], capture_output=True, text=True)
    module_path = user_site.stdout.strip()
    sys.path.append(module_path)

import paramiko
from infoworks.sdk.client import InfoworksClientSDK


def configure_logger():
    file_console_logger = logging.getLogger('File Archival')
    file_console_logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s — [ %(levelname)s ] - %(message)s')
    console_handler.setFormatter(formatter)
    file_console_logger.addHandler(console_handler)
    return file_console_logger


def check_file_or_dir_exists(sftp, path):
    try:
        # Attempt to retrieve file attributes
        sftp.stat(path)
        return True  # File exists
    except FileNotFoundError:
        return False  # File does not exist
    except Exception as error:
        print(f"Check_file_or_dir_exists - Error: {str(error)}")
        return False


def main():
    global logger
    logger = configure_logger()
    try:
        cwd = os.path.dirname(__file__)
        logger.info('Current Directory : ' + cwd)
        config_file = cwd + '/config.json'
        if os.path.exists(config_file):
            with open(config_file, "r") as f:
                configuration_json = json.load(f)
        else:
            logger.error(f"Error: {config_file} doesn't exist.")
            raise Exception("config.json does not exist")

        archival_base_path = configuration_json.get("archival_base_path", None)
        logger.info('Archival base path: ' + archival_base_path)
        private_key_name = configuration_json.get("private_key_name", None)
        logger.info('Private key name : ' + private_key_name)

        if archival_base_path is None or private_key_name is None:
            raise Exception("archival_base_path / private_key_name is not configured")

        source_id = os.environ.get('sourceId')
        table_id = os.environ.get('tableId')
        job_id = os.environ.get('jobId')
        # source_id = "65d43ff9fa21985156e58f9f"
        # table_id = "65d4404d681bff0007bac184"
        # job_id = "6617924468aeff1d391a562a"

        logger.info(f"Source ID: {source_id}")
        logger.info(f"Table ID: {table_id}")
        logger.info(f"Job ID: {job_id}")

        # Initialize InfoworksClientSDK
        iwx_client_obj = InfoworksClientSDK()
        iwx_client_obj.initialize_client_with_defaults(configuration_json.get('protocol', 'http'),
                                                       configuration_json.get('host', 'localhost'),
                                                       configuration_json.get('port', '3001'),
                                                       configuration_json.get("refresh_token", ""))

        # Retrieve source details
        src_details = iwx_client_obj.get_source_details(source_id=source_id)
        if src_details.get("result", {}).get("response", {}).get("result", {}).get("connection", {}):
            source_connection = src_details.get("result", {}).get("response", {}).get("result", {}).get("connection", {})
            sftp_host = source_connection.get("storage", {}).get("sftp_host", None)
            sftp_port = source_connection.get("storage", {}).get("sftp_port", None)
            sftp_username = source_connection.get("storage", {}).get("username", None)
        else:
            logger.error(f"Source Details Response : {src_details}")
            raise Exception("Failed to get source connection details")

        private_key_path = cwd + '/' + private_key_name
        logger.info('Private Key Path : ' + private_key_path)
        private_key_rsa = paramiko.RSAKey.from_private_key(open(private_key_path))

        # Create a transport object
        transport = paramiko.Transport((sftp_host, sftp_port))
        # Connect to the server
        transport.connect(username=sftp_username, pkey=private_key_rsa)
        # Create an SFTP session
        sftp = paramiko.SFTPClient.from_transport(transport)
        logger.info("Connected to SFTP server successfully.")

        current_date = datetime.datetime.now().strftime("%Y%m%d")
        archival_date_path = os.path.join(archival_base_path, current_date)
        logger.info('Base Archive Path with Date: ' + archival_date_path)
        archival_path = os.path.join(archival_date_path, job_id)
        logger.info('Target Archival Path: ' + archival_path)

        # Check if target directory exists
        if check_file_or_dir_exists(sftp, archival_base_path):
            print(f"Path {archival_base_path} exist.")
            if check_file_or_dir_exists(sftp, archival_date_path):
                print(f"Path {archival_date_path} exist.")
            else:
                print(f"Path {archival_date_path} does not exist. Creating directory")
                sftp.mkdir(archival_date_path)

            if check_file_or_dir_exists(sftp, archival_path):
                print(f"Path {archival_path} exist.")
            else:
                print(f"Path {archival_path} does not exist. Creating directory")
                sftp.mkdir(archival_path)
        else:
            logger.error(f"Path {archival_base_path} does not exist.")
            raise Exception(f"Path {archival_base_path} does not exist.")

        file_paths = iwx_client_obj.get_source_file_paths(source_id, table_id, job_id)

        for file_path in file_paths:
            logger.info(f"File Path: {file_path}")
            file_name = os.path.basename(file_path)
            archive_file_path = os.path.join(archival_path, file_name)
            logger.info(f"Archive File Path: {archive_file_path}")

            if check_file_or_dir_exists(sftp, file_path):
                sftp.rename(file_path, archive_file_path)
                logger.info(f"File '{file_path}' moved to '{archive_file_path}' successfully.")
            else:
                logger.error(f"Path {file_path} not available")

        sftp.close()
        transport.close()

        logger.info("Archive process completed successfully.")
    except Exception as error:
        logger.error(f"Failed to finish archival process - Error: {str(error)}")
        exit(-100)


if __name__ == "__main__":
    main()
