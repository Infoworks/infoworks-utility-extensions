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

def remove_comments_from_sql(sql_content):
    # Pattern to match both single-line and multi-line comments
    pattern = r"(--.*?$)|(/\*.*?\*/)"

    # Substitute the matched comments with an empty string
    cleaned_sql = re.sub(pattern, "", sql_content, flags=re.MULTILINE | re.DOTALL)

    # Remove any leading or trailing whitespace introduced by removed comments
    return cleaned_sql.strip()

def extract_session_parameters_with_values(file_path):
    #return []
    with open(file_path, "r") as file:
        sql_query = file.read()
    # Regular expression to match SET statements for session parameters
    pattern = r"SET\s+(\w+)\s*=\s*'([^']*)'"

    # Dictionary to store parameter names and values
    parameters_with_values = []

    # Find all matches of the pattern
    matches = re.findall(pattern, sql_query)

    # Add matches to the dictionary
    for param, value in matches:
        parameters_with_values.append({"key":param,"value": value})
    print("extract_session_parameters_with_values:",parameters_with_values)
    return parameters_with_values,matches

def extract_pre_exec_queries(file_path):
    #return []
    with open(file_path, "r") as file:
        sql_query = file.read()
    # Regular expression to match SET statements for session parameters
    pattern = r"(USE\s+(SCHEMA|DATABASE)\s+([\w]+));"

    # Dictionary to store parameter names and values
    pre_exec_queries=""
    # Find all matches of the pattern
    matches = re.findall(pattern, sql_query)
    print("matches:",matches)
    pre_exec_queries = ";".join([match[0] for match in matches])
    print("extracted_pre_exec_queries:",pre_exec_queries)
    return pre_exec_queries

def parse_sql_file(sql_file):
    """
    Function to extract individual sql statements from the SQL file
    """
    with open(sql_file) as infile:
        data = infile.read()
        sql_statements_list = data.split(";")
    sql_statements = []
    query_number = 0
    #print(sql_statements_list)
    for item in sql_statements_list:
        item=item.strip()
        item = remove_comments_from_sql(item)
        query_number = query_number + 1
        if bool(re.search(include_pattern, item, re.IGNORECASE)):
            sql_statements.append(item.strip())
        else:
            if item and not item.upper().startswith("USE") and not item.upper().startswith("SET"):
                skipped_sql_queries.append({"FILE_NAME": sql_file, "QUERY_NUMBER": query_number, "SQL_QUERY": item})
    skipped_sql_queries_df = pd.DataFrame(skipped_sql_queries)
    if not skipped_sql_queries_df.empty:
        skipped_sql_queries_df.to_csv("./skipped_sql_queries.csv",header=True)
    return sql_statements

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

    missing_keys = [key for key in required_keys if key not in data or data.get(key,"")==""]

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
      },
      {
        "entity_type": "entity_config",
        "entity_id": "672c47318160747c12435785",
        "configuration": {
          "id": "672c47318160747c12435784",
          "key": "dt_pre_execute_queries",
          "value": "{{ pre_exec_queries }}",
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
            "pipeline_parameters": {{ pipeline_params }},
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


pipeline_group_json_template = """{
    "id": "{{ id }}",
    "environment_id": "{{ environment_id }}",
    "domain_id": "{{ domain_id }}",
    "name": "{{ name }}",
    "description": "{{ description }}",
    "batch_engine": "{{ batch_engine }}",
    "run_job_on_data_plane": {{ run_job_on_data_plane | lower }},
    "pipelines": [
        {% for pipeline in pipelines %}
        {
            "pipeline_id": "{{ pipeline.pipeline_id }}",
            "version": {{ pipeline.version }},
            "execution_order": {{ pipeline.execution_order }},
            "run_active_version": {{ pipeline.run_active_version | lower }},
            "name": "{{ pipeline.name }}"
        }{% if not loop.last %},{% endif %}
        {% endfor %}
    ],
    "custom_tags": {{ custom_tags }},
    "query_tag": "{{ query_tag }}",
    "snowflake_profile": "{{ snowflake_profile }}",
    "snowflake_warehouse": "{{ snowflake_warehouse }}",
    "environment_configurations": {
        "environment_name": "{{ environment_name }}",
        "environment_compute_template_name": null,
        "environment_storage_name": null
    }
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
    config_json={}
    with open(config_json_file,"r") as f:
        config_json = json.load(f)
    required_keys = ["domain_name", "environment_name", "snowflake_profile","warehouse","input_directory","output_directory"]
    try:
        validate_json_keys(config_json, required_keys)
    except MissingKeyException as e:
        logger.error(f"Validation Error: {e}")
        exit(-100)
    input_directory = config_json.get("input_directory")
    output_directory = config_json.get("output_directory")
    pipeline_json_directory = os.path.join(output_directory,"pipeline")
    pipeline_modified_csv_directory = os.path.join(output_directory,"modified_files")
    pipeline_grp_json_directory = os.path.join(output_directory, "pipeline_group")
    pipeline_grp_modified_csv_directory = os.path.join(output_directory, "modified_files")
    domain_name = config_json.get("domain_name")
    with open(os.path.join(pipeline_modified_csv_directory, "pipeline.csv"), "w") as pipeline_csv_file,\
        open(os.path.join(pipeline_grp_modified_csv_directory, "pipeline_group.csv"), "w") as pipeline_grp_csv_file:
        pipeline_csv_writer = csv.writer(pipeline_csv_file)
        pipeline_grp_csv_writer = csv.writer(pipeline_grp_csv_file)
        for file_path in [os.path.join(input_directory, file) for file in os.listdir(input_directory)]:
            pipelines=[]

            if file_path.endswith('.sql'):
                session_parameters,matches = extract_session_parameters_with_values(file_path)
                pre_exec_queries = extract_pre_exec_queries(file_path)
                sql_statements = parse_sql_file(file_path)
                os.makedirs(pipeline_modified_csv_directory, exist_ok=True)
                for index,sql_statement in enumerate(sql_statements):
                    #print(sql_statement)
                    file_name_without_extension = file_path.split(os.sep)[-1].split(".")[0]
                    pipeline_id = bson.objectid.ObjectId()
                    pipeline_name = file_name_without_extension + "_" + str(index)
                    pipeline_template = Template(pipeline_json_template)
                    sql_query = encode_sql_to_base64(sql_query=sql_statement)
                    query_id = generate_query_id(sql_statement)
                    rendered_json = pipeline_template.render(
                        entity_type_pipeline="pipeline",
                        entity_id_pipeline=str(pipeline_id),
                        pipeline_name=pipeline_name,
                        pipeline_type= "sql",
                        entity_type_query="query",
                        entity_id_query=query_id,  # Replace with actual ID or UUID if needed
                        query= sql_query,
                        pre_exec_queries=pre_exec_queries,
                        entity_type_entity="pipeline",
                        entity_name=pipeline_name,
                        environment_name=config_json.get("environment_name"),
                        warehouse=config_json.get("warehouse"),
                        batch_engine=config_json.get("batch_engine","SNOWFLAKE"),
                        pipeline_params=json.dumps(session_parameters),
                        snowflake_profile=config_json.get("snowflake_profile"),
                        user_email=config_json.get("user_email", "admin@infoworks.io")
                    )
                    # Create the directory if it doesn't exist
                    os.makedirs(pipeline_json_directory, exist_ok=True)
                    output_file = os.path.join(pipeline_json_directory, f"{domain_name}#{pipeline_name}.json")
                    # Write the rendered JSON to a file
                    with open(output_file, "w") as file:
                        file.write(rendered_json)
                    temp={
                    "pipeline_id": str(pipeline_id),
                    "version": 1,
                    "execution_order": int(index)+1,
                    "run_active_version": True,
                    "name": pipeline_name
                    }
                    pipelines.append(temp)
                    logger.info(f"Rendered JSON written to {output_file}")
                    pipeline_csv_writer.writerow([f"{domain_name}#{pipeline_name}.json"])
                pipeline_grp_template = Template(pipeline_group_json_template)
                pg_id = bson.objectid.ObjectId()
                env_id = bson.objectid.ObjectId()
                domain_id = bson.objectid.ObjectId()
                pg_name = "pg" + "_" + file_name_without_extension
                pg_rendered_json = pipeline_grp_template.render(
                    id=pg_id,
                    environment_id=env_id,
                    domain_id=domain_id,
                    name=pg_name,
                    custom_tags=config_json.get("custom_tags", []),
                    query_tag=config_json.get("query_tag", ""),
                    environment_name=config_json.get("environment_name"),
                    snowflake_warehouse=config_json.get("warehouse"),
                    pipelines= pipelines,
                    run_job_on_data_plane=config_json.get("run_job_on_data_plane",False),
                    snowflake_profile=config_json.get("snowflake_profile"),
                    batch_engine=config_json.get("batch_engine", "SNOWFLAKE"),
                    user_email=config_json.get("user_email", "admin@infoworks.io")
                )
                # Create the directory if it doesn't exist
                os.makedirs(pipeline_grp_json_directory, exist_ok=True)
                os.makedirs(pipeline_grp_modified_csv_directory, exist_ok=True)
                output_file = os.path.join(pipeline_grp_json_directory, f"{domain_name}#{pg_name}.json")

                # Write the rendered JSON to a file
                with open(output_file, "w") as pg_file:
                    pg_file.write(pg_rendered_json)
                logger.info(f"Rendered JSON written to {output_file}")
                pipeline_grp_csv_writer.writerow([f"{domain_name}#{pg_name}.json"])
            else:
                logger.warning(f"Skipping {file_path} as its not sql file")
            #print(pipelines)
            #print(json.dumps(pipelines,indent=4))
    logger.info(f"Skipped SQLs if any are available at {cwd}/skipped_sql_queries.csv")

if __name__ == '__main__':
    main()