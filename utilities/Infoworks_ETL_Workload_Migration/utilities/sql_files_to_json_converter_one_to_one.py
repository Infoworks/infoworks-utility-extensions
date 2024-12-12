import logging
import traceback
import json
import os
import csv
import argparse
from jinja2 import Template
import bson
import base64
import re
import hashlib
import datetime
import threading
import pandas as pd

include_pattern = "^CREATE\s*(?:OR\s*REPLACE\s*)?(?:TEMPORARY\s*)?TABLE|^INSERT\s*INTO|^UPDATE|^DELETE\s*FROM|^DROP|^MERGE|^SET\s+([A-Z_]+)\s*=\s*\((SELECT.*)\)"
script_start_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

num_fetch_threads = 10

lock = threading.Lock()

skipped_sql_queries = []
dependency_analysis_output = []

read_dataframe = None
write_dataframe = None


def configure_logger(folder, filename):
    log_level = 'DEBUG'
    log_file = "{}/{}".format(folder, filename)
    if os.path.exists(log_file):
        os.remove(log_file)

    file_console_logger = logging.getLogger('BulkTableConfigure')
    file_console_logger.setLevel(log_level)

    formatter = logging.Formatter(
        "[%(asctime)s - %(levelname)s - %(filename)25s:%(lineno)s - %(funcName)20s() ] %(message)s")

    log_handler = logging.FileHandler(filename=log_file)
    log_handler.setLevel(log_level)
    log_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s — [ %(levelname)s ] - %(message)s')
    console_handler.setFormatter(formatter)

    file_console_logger.addHandler(log_handler)
    file_console_logger.addHandler(console_handler)

    print("Check the log file {} for detailed logs".format(log_file))
    return file_console_logger


def generate_query_id(sql_query):
    """
    Wrapper function that generates a hash of the sql query and generates a unique query id.
    """
    # Convert the SQL query to bytes
    query_bytes = sql_query.encode('utf-8')
    # Calculate the hash using SHA-1
    sha1_hash = hashlib.sha1(query_bytes).hexdigest()
    # Return the query ID
    return sha1_hash


# Function to read SQL file and encode its contents
def encode_sql_to_base64(sql_query):
    # Read the contents of the SQL file
    # with open(file_path, "r") as file:
    #     sql_query = file.read()
    # Encode the SQL query to base64
    encoded_query = base64.b64encode(sql_query.encode("utf-8")).decode("utf-8")

    return encoded_query


def parse_sql_file(sql_file):
    """
    Function to extract individual sql statements from the SQL file
    """
    with open(sql_file) as infile:
        data = infile.read()

    return data


class MissingKeyException(Exception):
    pass


def validate_json_keys(data, required_keys):
    """
    Validate if required keys are present in the JSON data.

    Parameters:
    - data: dict, the JSON data as a dictionary.
    - required_keys: list, keys that must be present in the data.

    Raises:
    - MissingKeyException if any key is missing.
    """

    missing_keys = [key for key in required_keys if key not in data or data.get(key, "") == ""]

    if missing_keys:
        raise MissingKeyException(f"Missing required keys: {', '.join(missing_keys)}")


# Define the JSON template with Jinja placeholders for the mandatory fields
pipeline_json_template = """
{
    "configuration": {
            "entity_configs": [
      {
        "entity_type": "entity_config",
        "entity_id": "672c472a8160747c12435783",
        "configuration": {
          "id": "672c472a8160747c12435783",
          "key": "dt_skip_table_metadata",
          "value": "true",
          "description": "",
          "is_active": true,
          "entity_id": "{{ entity_id_pipeline }}",
          "entity_type": "pipeline"
        }
      },
      {
        "entity_type": "entity_config",
        "entity_id": "672c47318160747c12435784",
        "configuration": {
          "id": "672c47318160747c12435784",
          "key": "dt_skip_sql_validation",
          "value": "true",
          "description": "",
          "is_active": true,
          "entity_id": "{{ entity_id_pipeline }}",
          "entity_type": "pipeline"
        }
      }
    ],
        "iw_mappings": [
            {
                "entity_type": "{{ entity_type_pipeline }}",
                "entity_id": "{{ entity_id_pipeline }}",
                "recommendation": {
                    "pipeline_name": "{{ pipeline_name }}"
                }
            },
            {
                "entity_type": "{{ entity_type_query }}",
                "entity_id": "{{ entity_id_query }}",
                "recommendation": {
                    "query": "{{ query }}"
                }
            },
      {
        "entity_type": "entity_config",
        "entity_id": "672c472a8160747c12435783",
        "recommendation": {
          "key": "dt_skip_table_metadata",
          "entity_name": "{{ entity_name }}",
          "entity_type": "pipeline"
        }
      },
      {
        "entity_type": "entity_config",
        "entity_id": "672c47318160747c12435784",
        "recommendation": {
          "key": "dt_skip_sql_validation",
          "entity_name": "{{ entity_name }}",
          "entity_type": "pipeline"
        }
      },
            {
        "entity_type": "entity_config",
        "entity_id": "672c47318160747c12435785",
        "recommendation": {
          "key": "dt_pre_execute_queries",
          "entity_name": "{{ entity_name }}",
          "entity_type": "pipeline"
        }
      }
        ],
        "entity": {
            "entity_type": "{{ entity_type_entity }}",
            "entity_id": "{{ entity_id_pipeline }}",
            "entity_name": "{{ entity_name }}",
            "environmentName": "{{ environment_name }}",
            "warehouse": "{{ warehouse }}"
        },
        "pipeline_configs": {
            "description": "",
            "type": "{{ pipeline_type }}",
            "query": "{{ query }}",
            "snowflake_profile": "{{ snowflake_profile }}",
            "batch_engine": "{{ batch_engine }}"
        }
    },
    "environment_configurations": {
        "environment_name": "{{ environment_name }}",
        "environment_compute_template_name": null,
        "environment_storage_name": null
    },
    "user_email": "{{ user_email }}"
}
"""


def main():
    parser = argparse.ArgumentParser(description='Convert SQL to JSON files')
    parser.add_argument('--config_json', required=True, type=str,
                        help='Pass the config json file path along with filename')
    args = parser.parse_args()
    config_json_file = args.config_json
    cwd = os.path.dirname(__file__)
    logs_folder = f"{cwd}/logs/"
    log_filename = "sql_files_to_json_converter.log"
    if not os.path.exists(logs_folder):
        os.makedirs(logs_folder)
    logger = configure_logger(logs_folder, log_filename)
    config_json = {}
    with open(config_json_file, "r") as f:
        config_json = json.load(f)
    required_keys = ["domain_name", "environment_name", "snowflake_profile", "warehouse", "input_directory",
                     "output_directory"]
    try:
        validate_json_keys(config_json, required_keys)
    except MissingKeyException as e:
        logger.error(f"Validation Error: {e}")
        exit(-100)
    input_directory = config_json.get("input_directory")
    output_directory = config_json.get("output_directory")
    pipeline_json_directory = os.path.join(output_directory, "pipeline")
    pipeline_modified_csv_directory = os.path.join(output_directory, "modified_files")
    # pipeline_grp_json_directory = os.path.join(output_directory, "pipeline_group")
    # pipeline_grp_modified_csv_directory = os.path.join(output_directory, "modified_files")
    domain_name = config_json.get("domain_name")
    with open(os.path.join(pipeline_modified_csv_directory, "pipeline.csv"), "w", newline='') as pipeline_csv_file:
        # open(os.path.join(pipeline_grp_modified_csv_directory, "pipeline_group.csv"), "w", newline='') as pipeline_grp_csv_file:
        pipeline_csv_writer = csv.writer(pipeline_csv_file)
        # pipeline_grp_csv_writer = csv.writer(pipeline_grp_csv_file)
        for file_path in [os.path.join(input_directory, file) for file in os.listdir(input_directory)]:
            pipelines = []

            if file_path.endswith('.sql'):
                # session_parameters,matches = extract_session_parameters_with_values(file_path)
                # pre_exec_queries = extract_pre_exec_queries(file_path)
                # sql_statements = parse_sql_file(file_path)
                sql_query = parse_sql_file(file_path)
                os.makedirs(pipeline_modified_csv_directory, exist_ok=True)
                # for index,sql_statement in enumerate(sql_statements):
                # print(sql_statement)
                file_name_without_extension = file_path.split(os.sep)[-1].split(".")[0]
                pipeline_id = bson.objectid.ObjectId()
                pipeline_name = file_name_without_extension
                pipeline_template = Template(pipeline_json_template)
                sql_query = encode_sql_to_base64(sql_query=sql_query)
                query_id = generate_query_id(sql_query)
                rendered_json = pipeline_template.render(
                    entity_type_pipeline="pipeline",
                    entity_id_pipeline=str(pipeline_id),
                    pipeline_name=pipeline_name,
                    pipeline_type="sql",
                    entity_type_query="query",
                    entity_id_query=query_id,  # Replace with actual ID or UUID if needed
                    query=sql_query,
                    entity_type_entity="pipeline",
                    entity_name=pipeline_name,
                    environment_name=config_json.get("environment_name"),
                    warehouse=config_json.get("warehouse"),
                    batch_engine=config_json.get("batch_engine", "SNOWFLAKE"),
                    snowflake_profile=config_json.get("snowflake_profile"),
                    user_email=config_json.get("user_email", "admin@infoworks.io")
                )
                # Create the directory if it doesn't exist
                os.makedirs(pipeline_json_directory, exist_ok=True)
                output_file = os.path.join(pipeline_json_directory, f"{domain_name}#{pipeline_name}.json")
                # Write the rendered JSON to a file
                with open(output_file, "w") as file:
                    file.write(rendered_json)

                logger.info(f"Rendered JSON written to {output_file}")
                pipeline_csv_writer.writerow([f"{domain_name}#{pipeline_name}.json"])

            else:
                logger.warning(f"Skipping {file_path} as its not sql file")
            # print(pipelines)
            # print(json.dumps(pipelines,indent=4))
    logger.info(f"Skipped SQLs if any are available at {cwd}/skipped_sql_queries.csv")


if __name__ == '__main__':
    main()
