import datetime
import json
import logging
import os
import argparse
import traceback

from azure.storage.blob import BlobServiceClient
import re
import time

from glob2regex import glob2regex
from infoworks.sdk.client import InfoworksClientSDK

global logger


def configure_logger():
    """
    Wrapper function to configure a logger to write to console and file.
    """
    file_console_logger = logging.getLogger('IWXFileWatcher')
    file_console_logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s — [ %(levelname)s ] - %(message)s')
    console_handler.setFormatter(formatter)
    file_console_logger.addHandler(console_handler)
    return file_console_logger


def check_done_file_exists(filename, blob_service_client, adls_container, file_suffix, ready_file_suffix):
    done_file = f"{filename.replace(file_suffix, '')}{ready_file_suffix}"
    logger.info(f"Following file found in source directory: {filename}")
    logger.info(f"Checking for its corresponding {done_file} file")

    blob_client = blob_service_client.get_blob_client(container=adls_container, blob=f"{done_file}")

    try:
        properties = blob_client.get_blob_properties()
        blob_client.close()
        return True
    except:
        return False


def check_done_file(ADLS_ACCOUNT, ADLS_CONTAINER, ADLS_DIRECTORY, FILENAME_REGEX, storage_account_key, file_suffix,
                    ready_file_suffix):
    if storage_account_key is not None:
        blob_service_client = BlobServiceClient(account_url=f"https://{ADLS_ACCOUNT}.blob.core.windows.net",
                                                credential=storage_account_key)
    else:
        logger.info("Switching to managed identity based authentication as the storage account key passed is NULL")
        blob_service_client = BlobServiceClient(account_url=f"https://{ADLS_ACCOUNT}.blob.core.windows.net")
    # Loop to check for .done files for filenames of interest
    blob_list = blob_service_client.get_container_client(ADLS_CONTAINER).list_blob_names(
        name_starts_with=ADLS_DIRECTORY)
    # Filter files based on regex pattern
    filtered_files = []
    for blob in blob_list:
        if FILENAME_REGEX.match(blob):
            filtered_files.append(blob)
    # Check if .done file exists for each filename of interest
    all_done = True
    for file in filtered_files:
        if not check_done_file_exists(file, blob_service_client, ADLS_CONTAINER, file_suffix, ready_file_suffix):
            all_done = False
            break

    blob_service_client.close()

    if all_done and len(filtered_files) > 0:
        logger.info("Process completed successfully.")
        return True
    else:
        if len(filtered_files) == 0:
            logger.error("No source files found")
        logger.info(f"Not all {ready_file_suffix} files found. Marking the table for retry")
        return False


def main():
    global logger
    logger = configure_logger()
    cwd = os.path.dirname(__file__)
    parser = argparse.ArgumentParser(description='File Watcher')
    parser.add_argument('--config_json_path', type=str, required=True,
                        help='Pass the absolute path of configuration json file')
    parser.add_argument('--source_ids', type=str, required=True,
                        help='Pass the source IDs in a comma-separated format for the tables that need to be ingested.')
    args = parser.parse_args()

    if os.path.exists(args.config_json_path):
        with open(args.config_json_path, "r") as f:
            configuration_json = json.load(f)
    else:
        logger.error(
            f"Specified configuration json path {args.config_json_path} doesn't exist. Please validate and rerun. Exiting..")
        exit(-100)

    RETRY_COUNT = configuration_json.get('retry_count', 36)  # Number of retries (36 * 5 minutes = 3 hours)
    RETRY_INTERVAL = configuration_json.get('retry_interval', 30)  # Sleep for 5 minutes = 300 seconds
    source_ids = args.source_ids.split(",")
    source_mappings = configuration_json.get("source_mappings", {})

    iwx_client_obj = InfoworksClientSDK()
    iwx_client_obj.initialize_client_with_defaults(configuration_json.get('protocol', 'http'),
                                                   configuration_json.get('host', 'localhost'),
                                                   configuration_json.get('port', '3001'),
                                                   configuration_json.get("refresh_token", ""))

    source_table_ingestion_status = {}
    for i in range(RETRY_COUNT):
        for source_id in source_ids:
            try:
                src_details = iwx_client_obj.get_source_details(source_id=source_id)
                source_name = src_details.get("result").get("response", {}).get("result", {}).get("name")
                src_sub_type = src_details.get("result").get("response", {}).get("result", {}).get("sub_type", "csv")
                logger.info(f"******** Working on Source: {source_name} ********")
                environment_id = source_mappings.get(source_name, {}).get("environment_id", None)
                compute_name = source_mappings.get(source_name, {}).get("compute_name", None)
                compute_id = iwx_client_obj.get_compute_id_from_name(environment_id, compute_name).get("result", {}).get(
                    "response", {}).get("compute_id", None)
                if environment_id is None or compute_name is None:
                    logger.error(
                        f"Please pass proper environment_id and compute_name under source section in configuration file for the source {source_name}. Skipping this source...")
                    continue
                if compute_id is None:
                    logger.error(
                        f"compute_id is None. Invalid environment_id and compute_name passed for the source {source_name}. Skipping this source...")
                    continue
                file_suffix = source_mappings.get(source_name, {}).get("file_suffix", ".csv.gz")
                ready_file_suffix = source_mappings.get(source_name, {}).get("ready_file_suffix", ".done")
                storage_account_key = source_mappings.get(source_name, {}).get("storage_account_key", None)

                source_base_path_relative = src_details.get("result", {}).get("response", {}).get("result", {}).get(
                    "connection", {}).get("source_base_path_relative", None)
                storage_details = src_details.get("result", {}).get("response", {}).get("result", {}).get("connection",
                                                                                                          {}).get(
                    "storage", {})
                storage_account_name = storage_details.get("storage_account_name", None)
                container = storage_details.get("file_system", None)
                if storage_account_name is None or container is None or source_base_path_relative is None:
                    logger.info(f"{source_base_path_relative is source_base_path_relative}")
                    logger.info(f"{storage_account_name is storage_account_name}")
                    logger.info(f"{container is container}")
                    logger.error("Invalid storage details for source {source_name}. Skipping this source...")
                    continue

                list_tables_response = iwx_client_obj.list_tables_in_source(source_id=source_id)
                tables = list_tables_response.get("result", {}).get("response", {}).get("result", [])
                logger.info(f"Found {len(tables)} tables under the source {source_name} to monitor/check.")
                for table in tables:
                    try:
                        include_filename_regex = f".*{file_suffix}"
                        table_id = table.get("id")
                        table_name = table.get("name", "")
                        if src_sub_type == "parquet":
                            adv_config_response = iwx_client_obj.get_list_of_advanced_config_of_table(source_id=source_id,
                                                                                                      table_id=table_id)
                            adv_config_of_table = adv_config_response.get("result", {}).get("response", {}).get("result", [])
                            for adv_config in adv_config_of_table:
                                key = adv_config.get('key', '')
                                value = adv_config.get('value', '')
                                if key.strip() == "additional_reader_properties_configuration":
                                    include_filename_regex = value.split(":")[-1]
                                    # Since this is glob pattern, convert it to regular regex pattern
                                    include_filename_regex = glob2regex(include_filename_regex)
                                    # include_filename_regex = ".*" + include_filename_regex.replace('?', '.').replace("*", '.*')

                        if source_table_ingestion_status.get(source_id, {}).get(table_id, "") == "done":
                            logger.info(f"The table {table_name}: {table_id} is already ingested. Skipping..")
                            continue
                        logger.info(f"------- Checking the source file for table: {table_name} -------")
                        regex_pattern = table.get("configuration", {}).get("include_filename_regex", include_filename_regex)
                        logger.info(
                            f"For the table: {table_name} the regex for the file matching is: {regex_pattern}")
                        source_relative_path = table.get("configuration", {}).get("source_relative_path")
                        source_relative_path = source_relative_path + "/" if not source_relative_path.endswith(
                            "/") else source_relative_path
                        adls_src_file_path = os.path.join(source_base_path_relative, source_relative_path)

                        status = check_done_file(storage_account_name, container, adls_src_file_path,
                                                 re.compile(rf"{regex_pattern}"), storage_account_key, file_suffix,
                                                 ready_file_suffix)
                        if status:
                            # Trigger the Ingestion job and mark the table as completed
                            logger.info(f"Triggered ingestion job for {table_id}")
                            submit_job_response = iwx_client_obj.submit_source_job(source_id=source_id, body={
                                "job_type": "truncate_reload",
                                "job_name": f'triggered_by_filewatch_{datetime.datetime.now().strftime("%Y%m%d%H%m%S")}',
                                "interactive_cluster_id": compute_id,
                                "table_ids": [
                                    table_id
                                ]
                            })
                            logger.info(submit_job_response)
                            source_table_ingestion_status.setdefault(source_id, {})[table_id] = "done"
                        else:
                            # Mark the table for retry
                            source_table_ingestion_status.setdefault(source_id, {})[table_id] = "retry"
                    except Exception as e:
                        logger.error(str(e))
                        traceback.print_exc()
            except Exception as e:
                logger.error(f"Polling of source {source_id} failed")
                logger.error(str(e))
                traceback.print_exc()

        # If any of the table needs a retry
        for values in source_table_ingestion_status.values():
            if "retry" in values.values():
                retry = True
                break
        else:
            retry = False

        if retry:
            logger.info(f"Few source files are not ready yet. Retrying in {RETRY_INTERVAL} seconds...")
            time.sleep(RETRY_INTERVAL)
        else:
            logger.info(
                "All the source files are ready and ingestion triggered successfully. Hence terminating gracefully")


if __name__ == '__main__':
    main()
