import argparse
import base64
import collections
import datetime
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import time
import traceback
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from infoworks.sdk.client import InfoworksClientSDK

include_pattern = "^CREATE\s*(?:TEMPORARY\s*)?TABLE|^INSERT\s*INTO|^UPDATE|^DELETE\s*FROM|^DROP|^MERGE"
script_start_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

num_fetch_threads = 10

lock = threading.Lock()

skipped_sql_queries = []
dependency_analysis_output = []

read_dataframe = None
write_dataframe = None


def get_schema_mappings(mapping_file):
    try:
        df = pd.read_csv(mapping_file, header=None)
        df.columns = ['Teradata_Schema', 'Teradata_Table', 'Snowflake_Schema', 'Snowflake_Table', 'Operation',
                      'Environment_Name', 'Ephemeral_Compute_Name', 'Interactive_Compute_Name', 'Snowflake_Warehouse',
                      'Snowflake_Profile']
        df['Teradata_Table_FQN'] = df['Teradata_Schema'] + '.' + df['Teradata_Table']
        df['Snowflake_Table_FQN'] = df['Snowflake_Schema'] + '.' + df['Snowflake_Table']
        write_dataframe = df[df['Operation'] == 'WRITE'].copy()
        read_dataframe = df[df['Operation'] == 'READ'].copy()

        write_dataframe.set_index('Teradata_Table_FQN', inplace=True)
        read_dataframe.set_index('Teradata_Table_FQN', inplace=True)
        return write_dataframe, read_dataframe
    except Exception as e:
        traceback.print_exc()
        return None, None


def list_files(base_dir):
    list_of_all_bteq_files = []
    if not os.path.exists(base_dir) or not os.path.isdir(base_dir):
        raise Exception(f"Invalid directory path given for all_bteq_base_directory config : {base_dir}")
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            file_path = os.path.join(root, file)
            list_of_all_bteq_files.append(file_path)
    return list_of_all_bteq_files


def get_modified_query(logger, query, parameter_values):
    env_name, snowflake_profile, snowflake_warehouse = None, None, None
    try:
        # Find any parameters in the table names
        match_params = re.finditer(r'(?:\$?{(.*?)}|\$([#\w]+)).(?:[\"#\w]+)', query, re.DOTALL)
        for match in match_params:
            match_grp = match.group()
            old_match_grp = match_grp
            match_param_with_grp = re.findall(r'{(.*?)}|\$([#\w]+)', match_grp, re.DOTALL)
            match_param_with_grp = [group[0] or group[1] for group in match_param_with_grp]
            for param_key in match_param_with_grp:
                value = parameter_values.get(param_key, "")
                if value != "":
                    match_grp = re.sub(r'{(.*?)}|\$([#\w]+)', value, match_grp)
            query = query.replace(old_match_grp, match_grp)

        pattern = r'\b\w+\.(?:["#\w]+)'
        matches = []
        for match in re.finditer(pattern, query):
            actual_match = match.group()
            matches.append((match.start(), match.end(), actual_match))

        if len(matches) > 0:
            sorted(matches, key=lambda x: x[0])
            filtered_matches_list = matches[::]

            match_to_check_for_write = matches[0]
            query_to_check_for_write = query[0:match_to_check_for_write[1]]
            write_pattern = r"(?:^INSERT\s*INTO|^UPDATE|^DELETE\s*FROM)\s+([\".#\w]+)"
            write_match = re.search(write_pattern, query_to_check_for_write, re.IGNORECASE)
            if write_match:
                table_name = write_match.group(1)
                updated_table_name = re.sub(r'["]+', '', table_name)
                updated_table_name = re.sub(r'[^.\w-]+', '_', updated_table_name)
                if updated_table_name in write_dataframe.index:
                    env_name = write_dataframe.loc[updated_table_name]['Environment_Name']
                    snowflake_warehouse = write_dataframe.loc[updated_table_name]['Snowflake_Warehouse']
                    snowflake_profile = write_dataframe.loc[updated_table_name]['Snowflake_Profile']
                    env_name = None if pd.isna(env_name) else env_name
                    snowflake_warehouse = None if pd.isna(snowflake_warehouse) else snowflake_warehouse
                    snowflake_profile = None if pd.isna(snowflake_profile) else snowflake_profile
                    replace_value = write_dataframe.loc[
                        updated_table_name, 'Snowflake_Table_FQN'] if updated_table_name in write_dataframe.index else table_name
                    if '"' in table_name:
                        query = query.replace(table_name, replace_value)
                    else:
                        table_name_to_find = table_name.replace('.', '\.')
                        query = re.sub(rf"\b{table_name_to_find}\b", replace_value, query)

                # Remove all the write matches from the matches list
                filtered_matches_list = [item for item in matches if item[-1] != table_name]

            tables_matched = []
            for item in filtered_matches_list:
                tables_matched.append(".".join(item[-1].split(".")[:2]))

            tables_matched = list(set(tables_matched))

            for table_name in tables_matched:
                updated_table_name = re.sub(r'["]+', '', table_name)
                updated_table_name = re.sub(r'[^.\w-]+', '_', updated_table_name)
                replace_value = read_dataframe.loc[
                    updated_table_name, 'Snowflake_Table_FQN'] if updated_table_name in read_dataframe.index else table_name
                if '"' in table_name:
                    query = query.replace(table_name, replace_value)
                else:
                    table_name_to_find = table_name.replace('.', '\.')
                    query = re.sub(rf"\b{table_name_to_find}\b", replace_value, query)
    except Exception as e:
        logger.error("Unable to convert the SQL query with schema mapping")
        logger.error(str(e))
        traceback.print_exc()
    finally:
        return query, env_name, snowflake_profile, snowflake_warehouse


def group_by_continuous_env(input_list):
    result = []
    current_group = []
    prev_env = None
    for item in input_list:
        env = item[5]
        if prev_env is None or env == prev_env:
            current_group.append(item)
        else:
            result.append(current_group)
            current_group = [item]
        prev_env = env
    if current_group:
        result.append(current_group)
    return result


def extract_comments(filepath):
    comments = []
    try:
        with open(filepath, 'r') as file:
            current_comment = ""
            inside_exec_block = False
            for line in file:
                stripped_line = line.strip()

                if stripped_line.startswith("#"):
                    # Add comment to current_comment string
                    current_comment += stripped_line.lstrip("#").strip() + "\n"
                elif stripped_line.startswith("exec"):
                    comments.append(current_comment.strip())
                    current_comment = ""
                    inside_exec_block = True
                elif inside_exec_block and stripped_line == '"""':
                    # Check for the end of exec block
                    inside_exec_block = False
                elif current_comment != "" and not (stripped_line.startswith("exec") or stripped_line.strip() == ""):
                    current_comment = ""
    except Exception as e:
        print(str(e))
        traceback.print_exc()
    comments = [comment if not set(comment) == {'-'} else "" for comment in comments]
    return comments


def parse_python_file(python_file):
    """
    The wrapper function is designed to parse the SnowConvert Python file.
    It extracts all the SQL statements found between the exec() function calls.
    Additionally, a valid include_pattern regex is applied to include SQL statements that match the pattern.
    """
    comments = extract_comments(python_file)
    with open(python_file) as infile:
        data = infile.read()
        matches = re.findall(r'exec\(f?"""(.*?)"""\)', data, re.DOTALL)
    matches = [item.strip() + ";" for item in matches]
    sql_statements = []
    query_number = 0
    for item in matches:
        # Remove any comments at the start of the line. Issue in latest snowct version
        if item.startswith('--'):
            item = "\n".join(item.split("\n")[1:]).strip()
        query_number = query_number + 1
        if bool(re.search(include_pattern, item, re.IGNORECASE)):
            try:
                comment = comments[query_number-1]
            except Exception as e:
                comment = ""
            sql_statements.append([comment, item.strip()])
        else:
            skipped_sql_queries.append({"FILE_NAME": python_file, "QUERY_NUMBER": query_number, "SQL_QUERY": item})
    return sql_statements


def create_pipeline_version(iwx_client, domain_id, pipeline_id, base64_string, pipeline_parameters, logger):
    """
    This is a wrapper function to create an Infoworks Pipeline version.
    """
    thread_name = threading.current_thread().name
    pipeline_version_id, version = None, None
    pv_response = iwx_client.create_pipeline_version(domain_id, pipeline_id, body={
        "pipeline_id": pipeline_id,
        "type": "sql",
        "query": base64_string,
        "pipeline_parameters": pipeline_parameters
    })
    if pv_response["result"].get("status", "") == "success":
        pipeline_version_id = pv_response["result"]["entity_id"]
        logger.info(f"{thread_name} - Pipeline version {pipeline_version_id} created")
        pv_details_response = iwx_client.get_pipeline_version_details(pipeline_id, domain_id,
                                                                      pipeline_version_id)
        version = pv_details_response["result"].get("response", {}).get("result", {}).get("version", None)
        # Make the new created version as active
        pv_active_response = iwx_client.set_pipeline_version_as_active(domain_id, pipeline_id,
                                                                       pipeline_version_id)
        if pv_active_response["result"].get("status", "") != "success":
            logger.error(f"{thread_name} - Unable to set the pipeline version {pipeline_version_id} as active")
            logger.error(str(pv_active_response))
        else:
            logger.info(f"{thread_name} - Pipeline version {pipeline_version_id} set as active")
    else:
        logger.error(f"{thread_name} - Unable to create the new pipeline version")
        logger.error(str(pv_response))

    return pipeline_version_id, version


def extract_table_name_from_sql(logger, sql_statement):
    """
    This is helper function to extract the target_table_name from the sql query.
    We use regex expressions to match the table_name
    Returns None if no regex is matched
    """
    try:
        sql_statement = sql_statement.strip()
        if bool(re.match(
                r"(?:CREATE|create)\s*(?:TEMPORARY|temporary)?\s*?(?:TABLE|table)\s*([\"#a-zA-Z0-9_$\{\}\.]+)\s*.*",
                sql_statement, re.IGNORECASE)):
            match = re.match(
                r"(?:CREATE|create)\s*(?:TEMPORARY|temporary)?\s*?(?:TABLE|table)\s*([\"#a-zA-Z0-9_$\{\}\.]+)\s*.*",
                sql_statement, re.IGNORECASE)
            table_name = match.group(1)
        elif bool(re.match(r"(?:INSERT|insert)\s*(?:INTO|into)\s*([\"#a-zA-Z0-9_$\{\}\.]+)\s*.*", sql_statement,
                           re.IGNORECASE)):
            match = re.match(r"(?:INSERT|insert)\s*(?:INTO|into)\s*([\"#a-zA-Z0-9_$\{\}\.]+)\s*.*", sql_statement,
                             re.IGNORECASE)
            table_name = match.group(1)
        elif bool(re.match(r"(?:UPDATE|update)\s*([\"#a-zA-Z0-9_$\{\}\.]+)\s*.*", sql_statement, re.IGNORECASE)):
            match = re.match(r"(?:UPDATE|update)\s*([\"#a-zA-Z0-9_$\{\}\.]+)\s*.*", sql_statement, re.IGNORECASE)
            table_name = match.group(1)
        elif bool(re.match(r"(?:DELETE|delete)\s*FROM\s*([\"#a-zA-Z0-9_$\{\}\.]+)\s*.*", sql_statement, re.IGNORECASE)):
            match = re.match(r"(?:DELETE|delete)\s*FROM\s*([\"#a-zA-Z0-9_$\{\}\.]+)\s*.*", sql_statement, re.IGNORECASE)
            table_name = match.group(1)
        elif bool(re.match(r"(?:DROP|drop)\s*(?:TABLE|table)\s*([\"#a-zA-Z0-9_$\{\}\.]+)\s*.*", sql_statement,
                           re.IGNORECASE)):
            match = re.match(r"(?:DROP|drop)\s*(?:TABLE|table)\s*([\"#a-zA-Z0-9_$\{\}\.]+)\s*.*", sql_statement,
                             re.IGNORECASE)
            table_name = match.group(1)
        else:
            logger.error("Unidentified SQL statement")
            logger.error(sql_statement)
            table_name = None
        return table_name
    except Exception as e:
        traceback.print_exc()
        logger.error(str(e))
        return None


def get_dependency_analysis(env_id, pipeline_id, database_name, target_table_name, sql_statement):
    try:
        write_type = ""
        if sql_statement.replace('USE $DB_NAME;', '').strip().upper().startswith("CREATE"):
            write_type = "CREATE"
        elif sql_statement.replace('USE $DB_NAME;', '').strip().upper().startswith("INSERT"):
            write_type = "INSERT"
        elif sql_statement.replace('USE $DB_NAME;', '').strip().upper().startswith("UPDATE"):
            write_type = "UPDATE"
        elif sql_statement.replace('USE $DB_NAME;', '').strip().upper().startswith("DELETE"):
            write_type = "DELETE"
        elif sql_statement.replace('USE $DB_NAME;', '').strip().upper().startswith("DROP"):
            write_type = "DROP"
        source_table_names = []
        pattern = r"(?<!DELETE\s)\bFROM\s+(?:(?:[\"#_\w]+\.[\"\w_#]+)(?:[\"#\s\w]*,\s+)?)+"
        matches = re.findall(pattern, sql_statement, re.IGNORECASE)
        if len(matches) > 0:
            for item in matches:
                source_table_names.append(item.strip().replace("FROM", "").strip().split()[0])

        write_access_table = f"{database_name}." + target_table_name
        read_access_tables = ','.join(list(set([f"{database_name}." + i for i in source_table_names])))
        # print(f"User configured in {env_id} needs write access to {write_access_table}")
        # print(f"User configured in {env_id} needs read access to {read_access_tables}")
        lock.acquire()
        dependency_analysis_output.append(
            {"pipeline_id": pipeline_id, "environment_id": env_id, "write_type": write_type,
             "write_tables": write_access_table,
             "read_tables": read_access_tables})
        lock.release()
    except Exception as e:
        print(f"Unable to get the dependency analysis for {pipeline_id}: " + str(e))


def create_infoworks_pipeline(logger, iwx_client, sql, comment, env_id, domain_id, snowflake_profile, warehouse, filename,
                              pipeline_prefix,
                              pipeline_parameters, parameter_values,
                              create_or_overwrite_pv,
                              pl_count, tgt_table_name, tgt_table_for_dependency_analysis):
    thread_name = threading.current_thread().name
    pipeline_id, pipeline_version_id, version = None, None, None
    new_pl_created = False
    database_name = pipeline_parameters[0].get("value")

    """
    This code block is designed to identify and extract pipeline parameters. 
    The logic involves searching for text enclosed within curly braces {}. 
    When a variable is found, the corresponding key-value pair is added to the `pipeline_parameters` array.
    """
    # match_params = re.findall(r'{(.*?)}|\$(.*?)(?:;|\s|\n)', sql, re.DOTALL)
    match_params = re.findall(r'{(.*?)}|\$([#\w]+)', sql, re.DOTALL)
    result = [group[0] or group[1] for group in match_params]
    match_params = list(set(result))
    try:
        match_params.remove("DB_NAME")
    except ValueError:
        pass
    if len(match_params) > 0:
        for param_key in match_params:
            pipeline_parameters.append({"key": param_key, "value": parameter_values.get(param_key, "")})

    """
    If the value of `tgt_table_name` is None, a random name is generated. 
    Otherwise, the code checks if there are any pipeline parameters present in the `tgt_table_name`.  Example: {SRCDB}.sls_hier_brg_dim
    If pipeline parameters are found, they are split using the . delimiter, and the last item in the resulting list is extracted as the tgt_table_name.
    """
    if tgt_table_name is None:
        random_uuid = uuid.uuid4()
        tgt_table_name = f"iwx_tgt_table_{random_uuid}"
    else:
        params_in_tgt_table_name = re.findall(r'{(.*?)}', tgt_table_name, re.DOTALL)
        # params_in_tgt_table_name = [group[0] or group[1] for group in params_in_tgt_table_name]
        if len(params_in_tgt_table_name) > 0:
            tgt_table_name = re.sub(r"\{([A-Za-z_0-9\s]+)\}", r"\1", tgt_table_name)
        tgt_table_name = tgt_table_name.split(".")[-1]

    """
    Create pipeline using the pipeline_config as shown below
    Naming convention used for pipeline is pl-dit-<platform >-<app group(cnt)>-<mots_id>-<app area>-<pipeline_name>
    pipeline_name is <BTEQFileName>_<TargetTableName>_<sqlnumber>
    In case the pipeline creation fails, the code checks for any existing pipelines with the same name and retrieves the pipeline ID for downstream use.
    """
    pipeline_suffix = f"{os.path.splitext(filename)[0].replace('_BTEQ', '')}_{tgt_table_name.upper()}_{pl_count}"
    pipeline_name = f"{pipeline_prefix}-{pipeline_suffix}"
    pipeline_name = re.sub(r'[^#\w-]+', '_', pipeline_name)
    pipeline_config = {
        "name": pipeline_name,
        "description": '',
        "environment_id": env_id,
        "domain_id": domain_id,
        "custom_tags": [],
        "batch_engine": 'SNOWFLAKE',
        "snowflake_profile": snowflake_profile,
        "snowflake_warehouse": warehouse,
        "run_job_on_data_plane": False,
        "query_tag": ""
    }
    if snowflake_profile is None:
        del pipeline_config['snowflake_profile']
    pipeline_create_response = iwx_client.create_pipeline(pipeline_config=pipeline_config)
    pipeline_get_response = None
    if pipeline_create_response["result"].get("status", "") == "success":
        pipeline_id = pipeline_create_response["result"]["pipeline_id"]
        new_pl_created = True
    else:
        # Check if there is any existing pipeline with name
        pipeline_get_response = iwx_client.get_pipeline_id(pipeline_name, domain_id)
        if pipeline_get_response["result"]["status"].upper() == "SUCCESS":
            pipeline_id = pipeline_get_response["result"]["pipeline_id"]

    """
    If we have a valid `pipeline_id` based on the value of flag `create_pipeline_version` create or update the pipeline version
    """
    if pipeline_id is not None:
        get_dependency_analysis(env_id, pipeline_id, database_name, tgt_table_for_dependency_analysis, sql)
        logger.info(f"{thread_name} - Pipeline {pipeline_name} with id: {pipeline_id} created/found")
        # Add/Update advance configuration for pipeline
        iwx_client.modify_advanced_config_for_pipeline(domain_id, pipeline_id, adv_config_body={
            "key": "dt_skip_sql_validation",
            "value": "true",
            "description": "Advance configuration to disable SQL validation",
            "is_active": True
        }, action_type="create")

        # sql = sql.replace("{", "$").replace("}", "")
        sql = re.sub(r"\{([A-Za-z_0-9\s]+)\}", r"$\1", sql).replace("$$", "$")
        #  I think this is the right place to add comments to the SQL statement after all the modifications are done to the SQL.
        sql = sql if comment == "" else "/*\n" + comment + " */\n" + sql
        sample_string_bytes = sql.encode("ascii")
        base64_bytes = base64.b64encode(sample_string_bytes)
        base64_string = base64_bytes.decode("ascii")
        if new_pl_created or create_or_overwrite_pv.lower() == "c":
            pipeline_version_id, version = create_pipeline_version(iwx_client, domain_id, pipeline_id, base64_string,
                                                                   pipeline_parameters, logger)
        elif create_or_overwrite_pv.lower() == "u":
            # Find the existing active sql version of the pipeline and update the SQL query
            logger.info(f"{thread_name} - Trying to get the active version of the pipeline that also is a SQL pipeline")
            # TO-DO
            # params = {"filter": {"is_active": True, "type": "sql"}}
            params = {"filter": {"type": "sql"}, "order_by": "desc", "sort_by": "version"}
            pv_list_response = iwx_client.list_pipeline_versions(domain_id=domain_id,
                                                                 pipeline_id=pipeline_id,
                                                                 params=params)
            pv_list = pv_list_response["result"].get("response", {}).get("result", [])
            if pv_list_response["result"].get("status", "") == "success" and len(pv_list) > 0:
                pv_active = pv_list[0]["id"]
                pipeline_version_id = pv_active
                logger.info(f"{thread_name} - Got the active pipline version id: {pv_active}")
                version = pv_list[0]["version"]
                body = {"type": "sql", "query": base64_string, "pipeline_parameters": pipeline_parameters}
                pv_update_response = iwx_client.update_pipeline_version(domain_id=domain_id, pipeline_id=pipeline_id,
                                                                        pipeline_version_id=pv_active, body=body)
                if pv_update_response["result"].get("status", "") == "success":
                    logger.info(f"{thread_name} - Updated the pipeline version with latest SQL {pv_active}")
                else:
                    logger.error(
                        f"{thread_name} -  Unable to update the pipeline version with latest SQL. Pipeline {pipeline_id} and Pipeline Version {pv_active}")
                    logger.error(str(pv_update_response))
            else:
                logger.error(f"{thread_name} - Unable to find existing pipeline version that is a SQL pipeline")
                pipeline_version_id, version = create_pipeline_version(iwx_client, domain_id, pipeline_id,
                                                                       base64_string, pipeline_parameters, logger)
    else:
        logger.error(f"{thread_name} - Unable to create/find the pipeline: {pipeline_name}")
        logger.error(str(pipeline_create_response))
        if pipeline_get_response is not None:
            logger.error(str(pipeline_get_response))
    return pipeline_id, pipeline_name, pipeline_version_id, version, pipeline_parameters


def create_iwx_pipeline_group(logger, iwx_client, pipeline_group_name, environment_id, domain_id, snowflake_profile,
                              snowflake_warehouse,
                              pipelines_created):
    pipeline_group_id = None
    pipeline_group_id_status = "SUCCESS"
    try:
        pipeline_group_config = {
            "name": pipeline_group_name,
            "description": "",
            "environment_id": environment_id,
            "domain_id": domain_id,
            "batch_engine": "SNOWFLAKE",
            "run_job_on_data_plane": False,
            "pipelines": [{
                "pipeline_id": items[1],
                "version": items[4],
                "execution_order": i + 1,
                "run_active_version": False
            } for i, items in enumerate(pipelines_created)],
            "custom_tags": [],
            "query_tag": "",
            "snowflake_profile": snowflake_profile,
            "snowflake_warehouse": snowflake_warehouse

        }
        if snowflake_profile is None:
            del pipeline_group_config['snowflake_profile']
        pg_response = iwx_client.create_pipeline_group(
            pipeline_group_config=pipeline_group_config)
        if pg_response["result"]["status"].upper() == "SUCCESS":
            logger.info("Pipeline group has been created successfully")
            pipeline_group_id = pg_response["result"].get("entity_id", None)
        else:
            logger.error("Pipeline group creation failed")
            logger.error(str(pg_response))
            # Check if there is already pipeline group with the same name
            logger.info("Checking if there exists any pipeline group with the same name")
            pg_list_response = iwx_client.list_pipeline_groups_under_domain(domain_id, params={
                "filter": {"name": pipeline_group_name}})
            if pg_list_response["result"]["status"].upper() == "SUCCESS":
                pg_list = [i.get("pipeline_group_details") for i in
                           pg_list_response["result"]["response"].get("result", [])]
                if len(pg_list) > 0:
                    pipeline_group_id = pg_list[0].get("id", None)
                    logger.info(
                        f"Found a pipeline group with {pipeline_group_id} with the name {pipeline_group_name}")
                if pipeline_group_id is not None:
                    pg_update_response = iwx_client.update_pipeline_group(domain_id,
                                                                          pipeline_group_id,
                                                                          pipeline_group_config=pipeline_group_config)
                    if pg_update_response["result"]["status"].upper() == "SUCCESS":
                        logger.info(f"Pipeline group {pipeline_group_id} updated successfully")
                    else:
                        logger.error(f"Updating of Pipeline group {pipeline_group_id} failed")
                        logger.error(str(pg_update_response))
            else:
                pipeline_group_id_status = "ERROR"
                logger.error(
                    f"Unable to list the pipeline groups and find the pipeline group id with name: {pipeline_group_name}")
                logger.error(str(pg_list_response))
    except Exception as e:
        logger.error("Something failed while creating/updating the pipeline group")
        pipeline_group_id_status = "ERROR"
        logger.error(str(e))
        traceback.print_exc()
    finally:
        return pipeline_group_id, pipeline_group_id_status


def configure_logger(folder, filename):
    """
    Wrapper function to configure a logger to write to console and file.
    """
    log_file = "{}/{}".format(folder, filename)
    if os.path.exists(log_file):
        os.remove(log_file)
    file_console_logger = logging.getLogger('OnTheFlyPipelines')
    file_console_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "[%(asctime)s - %(levelname)s - %(filename)25s:%(lineno)s - %(funcName)20s() -  %(message)s")
    log_handler = logging.FileHandler(filename=log_file)
    log_handler.setLevel(logging.DEBUG)
    log_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
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


def remove_directory(directory_path):
    try:
        for root, dirs, files in os.walk(directory_path, topdown=False):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                os.rmdir(dir_path)
        os.rmdir(directory_path)
        print(f"Directory '{directory_path}' and its contents removed successfully.")
    except OSError as e:
        print(f"Error while removing directory '{directory_path}': {e}")


def migrate_folder(logger, snowct_binary_path, list_of_failed_bteqs, input_dir, output_dir):
    conversion_status_dict = {}
    """
    Copy the failed BTEQ files from their original location to the input directory, ensuring that the JCL directory structure is maintained during the process.
    Example: The BTEQ file /home/etlap12/iwbteq/bteq_sql_import/sql/sdralljcls/ZQ0M0J1C/1_ZQ0M0G00.sql is copied to /input_dir/ZQ0M0J1C/1_ZQ0M0G00.sql and so on...
    """
    for file in list_of_failed_bteqs:
        jcl_subdir = os.path.basename(os.path.dirname(file))
        input_subdir = os.path.join(input_dir.rstrip("/"), jcl_subdir)
        try:
            os.makedirs(input_subdir)
        except FileExistsError:
            logger.info(f"The directory '{input_subdir}' already exists.")
        except Exception as e:
            logger.error(f"Unable to create '{input_subdir}'.")
            logger.error(str(e))

        try:
            tgt_file_name = os.path.splitext(os.path.basename(file))[0] + ".bteq"
            shutil.copy(file, os.path.join(input_subdir, tgt_file_name))
        except FileNotFoundError as e:
            logger.error(f"Unable to move '{file}' to '{input_subdir}'. The file was not found.")
            logger.error(str(e))
        except PermissionError as e:
            logger.error(f"Unable to move '{file}' to '{input_subdir}'. Permission denied.")
            logger.error(str(e))
        except Exception as e:
            logger.error(f"Unable to move '{file}' to '{input_subdir}'.")
            logger.error(str(e))

    """
    Execute the snowct command for the input directory
    Example: snowct -i /input_dir/ -o /output_dir/ -p Python
    Output files are stored under /output_dir/Output directory with JCL structure maintained
    """
    try:
        os.makedirs(output_dir)
    except FileExistsError:
        logger.info(f"The directory '{output_dir}' already exists.")
        remove_directory(os.path.join(output_dir, 'Output'))
    except Exception as e:
        logger.error(f"Unable to create '{output_dir}'.")
        logger.error(str(e))

    conversion_status = "success"
    command = f"{snowct_binary_path} -i {input_dir} -o {output_dir} -p Python"
    logger.info(f"Running command: {command}")
    try:
        os.system(command)
        logger.info(f"Successfully executed the command {command}")
    except Exception as e:
        logger.info(f"Execution of the command {command} failed")
        logger.error(str(e))
        conversion_status = "failed"
    finally:
        conversion_status_dict[output_dir] = conversion_status
    return conversion_status_dict


def thread_callable(params):
    file_full_path = ""
    try:
        thread_name = threading.current_thread().name
        query_position = params['query_position']
        comment = params['comment']
        sql = params['sql']
        dry_run = params['dry_run']
        logger = params['logger']
        filename = params['filename']
        file_full_path = params['file_full_path']
        iwx_client = params['iwx_client']
        environment_id = params['environment_id']
        domain_id = params['domain_id']
        snowflake_profile = params['snowflake_profile']
        snowflake_warehouse = params['snowflake_warehouse']
        pipeline_prefix = params['pipeline_prefix']
        parameter_values = params['parameter_values']
        temp_table_stage_mapping = params['temp_table_stage_mapping']
        database_name = params['database_name']
        create_or_overwrite_pipeline_version = params['create_or_overwrite_pipeline_version']
        jcl_name = params['jcl_name']
        tgt_table_name_list = params['tgt_table_name_list']
        pipeline_grp_param_list = params['pipeline_grp_param_list']
        pipelines_created = params['pipelines_created']
        basic_report_output = params['basic_report_output']
        sql_query_num_mappings = params['sql_query_num_mappings']
        query_id = generate_query_id(sql)
        query_number = query_position + 1
        tgt_table_name = extract_table_name_from_sql(logger, sql)
        tgt_table_for_dependency_analysis = tgt_table_name
        if tgt_table_name is not None:
            tgt_table_name = tgt_table_name.split('.')[-1].upper()
            tgt_table_name = re.sub(r'"', '', tgt_table_name)

        lock.acquire()
        tgt_table_name_list.append(tgt_table_name)
        lock.release()
        sql = f"USE $DB_NAME;\n" + sql.strip(";") + ";"

        for item in temp_table_stage_mapping:
            sql = sql.replace(item, temp_table_stage_mapping[item] + "." + item + " ")
            # comment = comment.replace(item, temp_table_stage_mapping[item] + "." + item + " ")
            tgt_table_for_dependency_analysis = tgt_table_for_dependency_analysis.replace(item,
                                                                                          temp_table_stage_mapping[item] + "." + item)

        sql_to_print = sql if comment == "" else "\n/*\n" + comment + " */\n" + sql
        logger.debug(
            f'{thread_name} -- File: {filename} Statement {query_number} Target Table Name: {tgt_table_name} and SQL: {sql_to_print}')
        lock.acquire()
        sql_query_num_mappings[query_number] = sql
        lock.release()

        if not dry_run:
            pipeline_id, pipeline_name, active_version_id, version, pipeline_parameters = create_infoworks_pipeline(
                logger,
                iwx_client,
                sql,
                comment,
                environment_id,
                domain_id,
                snowflake_profile,
                snowflake_warehouse,
                filename,
                pipeline_prefix,
                [{
                    "key": "DB_NAME",
                    "value": database_name
                }],
                parameter_values,
                create_or_overwrite_pipeline_version,
                query_position + 1, tgt_table_name, tgt_table_for_dependency_analysis)
            temp_result = {"SNOWSQL_FILE_NAME": file_full_path, "SQL_QUERY_ID": query_id,
                           "QUERY_NUMBER": query_position + 1, "JCL_NAME": jcl_name,
                           "PIPELINE_ID": pipeline_id, 'ENVIRONMENT_ID': environment_id,
                           'SNOWFLAKE_WAREHOUSE': snowflake_warehouse,
                           "TARGET_TABLE_NAME": tgt_table_name.split('.')[-1].upper(),
                           "PARSING_STATUS": "SUCCESS"}
            lock.acquire()
            pipeline_grp_param_list.append({pipeline_id: pipeline_parameters})
            lock.release()
            if None not in {pipeline_id, active_version_id, version}:
                lock.acquire()
                pipelines_created.append(
                    (query_position, pipeline_id, pipeline_name, active_version_id, version, environment_id,
                     snowflake_profile, snowflake_warehouse))
                lock.release()
            else:
                logger.error(
                    f"{thread_name} -- Either of the pipeline_id: {pipeline_id}, active_version_id: {active_version_id}, version: {version} is None ")
                logger.error(f"{thread_name} -- Not adding the above SQL Statement into list of pipelines created")
                temp_result["PARSING_STATUS"] = "FAILED"
            lock.acquire()
            basic_report_output.append(temp_result)
            lock.release()
    except Exception as e:
        traceback.print_exc()
        print(f"Thread failed due to exception while processing {file_full_path} {str(e)}")


def main():
    parser = argparse.ArgumentParser(description='On the fly pipelines')
    parser.add_argument('-c', '--config_file', required=True, type=str,
                        help='Pass the fully qualified path of configuration file')
    parser.add_argument('-r', '--refresh_token', required=False, type=str,
                        help='Pass the refresh token of the Infoworks User here. This token is used for automation')
    parser.add_argument('-b', '--bteq_result_file', required=False, type=str,
                        help='Pass the fully qualified path of bteq_result csv dump incase you need the script to automate the snowconvert process')
    parser.add_argument('-s', '--schema_mapping_file', required=False, type=str,
                        help='Pass the fully qualified path of csv dump with schema mapping')
    args = vars(parser.parse_args())

    config_file_path = args.get("config_file")
    refresh_token_from_args = args.get("refresh_token", None)
    bteq_result_file = args.get("bteq_result_file", None)
    schema_mapping_file = args.get("schema_mapping_file", None)
    global read_dataframe
    global write_dataframe
    if schema_mapping_file is not None and os.path.exists(schema_mapping_file):
        write_dataframe, read_dataframe = get_schema_mappings(schema_mapping_file)
    else:
        write_dataframe, read_dataframe = None, None

    if not os.path.exists(config_file_path):
        raise Exception(f"{config_file_path} not found")
    try:
        config_file = open(config_file_path)
        config = json.load(config_file)
        logs_folder = config.get('logs_folder', "/tmp")
        log_filename = '{}.{}.log'.format("snowsql_iwx_converter", script_start_time)
        logger = configure_logger(logs_folder, log_filename)
        host = config.get('host', None)
        port = config.get('port', None)
        protocol = config.get('protocol', 'https')
        default_environment_details = config.get('default_environment_details', None)
        domain_name = config.get('domain_name', None)
        database_name = config.get('database_name', None)
        refresh_token = config.get('refresh_token', refresh_token_from_args)
        dry_run = config.get('dry_run', False)
        skip_migration = config.get('skip_migration', True)
        snowct_binary_path = config.get('snowct_binary_path', "snowct")
        pipeline_prefix = config.get('pipeline_prefix', "iwx_pipeline")
        pipeline_group_prefix = config.get('pipeline_group_prefix', "iwx_pipeline_group")
        workflow_prefix = config.get('workflow_prefix', "iwx_workflow")
        parameter_values = config.get('parameter_values', {})
        db_stage_mappings = config.get('db_stage_mappings', {})
        create_or_overwrite_pipeline_version = config.get('create_or_overwrite_pipeline_version', "create")
        output_directory = config.get('output_directory', None)
        input_directory = config.get('raw_input_directory', None)

        if not skip_migration:
            if input_directory is None or output_directory is None:
                raise Exception(f"output_directory or raw_input_directory is None. Please pass valid value")
        else:
            if output_directory is None:
                raise Exception(f"output_directory is None. Please pass valid value")

        flow = "python"
        if None in {host, port, refresh_token, domain_name, database_name} or default_environment_details is None:
            logger.error(
                "host/port/refresh_token/default_environment_details/domain_name/database_name cannot be None")
            sys.exit(-1)

        environment_details_with_ids = {}
        if not dry_run:
            iwx_client = InfoworksClientSDK()
            iwx_client.initialize_client_with_defaults(protocol, host, port, refresh_token)
            # Get environment id from environment name
            for environment_name in default_environment_details:
                env_response = iwx_client.get_environment_id_from_name(environment_name)
                if env_response["result"]["response"]["environment_id"] is not None:
                    environment_id = env_response["result"]["response"]["environment_id"]
                    environment_details_with_ids[environment_id] = {**default_environment_details[environment_name],
                                                                    "environment_name": environment_name}
                    logger.info(f"Got environment id {environment_id} for {environment_name}")
                else:
                    logger.error(f"Unable to get environment id for {environment_name}")
                    logger.error(str(env_response))
                    sys.exit(-1)

            # Get domain id from domain name
            domain_response = iwx_client.get_domain_id(domain_name)
            domain_id = domain_response["result"]["response"].get("domain_id", None)
            if domain_id is None:
                logger.error(f"Unable to get domain id for {domain_name}")
                logger.error(str(domain_response))
                sys.exit(-1)
            else:
                logger.info(f"Got domain id {domain_id} for {domain_name}")
        else:
            environment_id, domain_id, iwx_client= None, None, None
        basic_report_output = []
        pipeline_pipeline_grp_mappings = {}
        pg_status_mappings = {}
        pg_wf_mappings = {}

        if not skip_migration:
            if bteq_result_file is None:
                all_bteq_base_directory = config.get("all_bteq_base_directory", None)
                if all_bteq_base_directory is None:
                    raise Exception(
                        "You have to provide all_bteq_base_directory config in config.json if you have set skip_migration config to false.\n"
                        " This would convert all the files under all_bteq_base_directory")
                list_of_all_bteqs = list_files(all_bteq_base_directory)
                conversion_status_dict = migrate_folder(logger, snowct_binary_path, list_of_all_bteqs,
                                                        input_directory,
                                                        output_directory)
            else:
                df = pd.read_csv(bteq_result_file)
                df_temp = df.query(
                    "`Parse Status` == 'Failed' | `Metadata Job Status` == 'Failed' | `SQL Import` == 'Failed'")
                list_of_failed_bteqs = df_temp['File Name'].unique()
                conversion_status_dict = migrate_folder(logger, snowct_binary_path, list_of_failed_bteqs,
                                                        input_directory,
                                                        output_directory)
            for key in conversion_status_dict:
                logger.info(
                    f"Status of conversion of the BTEQs in directory {key} to SnowSQLs is : {conversion_status_dict[key].upper()}")

        directory_to_scan = os.path.join(output_directory, "Output")
        jcl_pipeline_group_mappings = {}
        dry_run_report = {}
        logger.info(f"Recursively scanning the directory {directory_to_scan} for {flow} files")

        for root, directories, files in os.walk(directory_to_scan):
            # Iterate over the files in the current directory
            for filename in files:
                file_full_path = os.path.join(root, filename)
                jcl_name = os.path.basename(os.path.dirname(file_full_path))
                pipelines_created = []
                tgt_table_name_list = []
                query_number = None
                try:
                    if filename.lower().endswith(".sql" if flow == "sql" else ".py"):
                        logger.info(f"Got {file_full_path} to scan")
                        logger.info(f"JCL for the {file_full_path} is {jcl_name}")

                        parsed_items = parse_python_file(file_full_path)
                        if len(parsed_items) == 0:
                            logger.exception(f"The file {filename} has no valid SQL statements. Parsing failed")
                            raise Exception("File parsing error")

                        # Find all the temporary tables present within the BTEQ and map it with their stage database
                        temp_table_stage_mapping = {}
                        for comment, sql_statement in parsed_items:
                            if bool(re.match(
                                    r"(?:CREATE|create)\s*(?:TEMPORARY|temporary)?\s*?(?:TABLE|table)\s*([\"#a-zA-Z0-9_$\{\}\.]+)\s*.*",
                                    sql_statement, re.IGNORECASE)):
                                match = re.match(
                                    r"(?:CREATE|create)\s*(?:TEMPORARY|temporary)?\s*?(?:TABLE|table)\s*([\"#a-zA-Z0-9_$\{\}\.]+)\s*.*",
                                    sql_statement, re.IGNORECASE)
                                temp_table_name = match.group(1)
                                # get_main_table_regex = r"\bFROM\s+([^\s]+)"
                                # get_main_table_regex = r'\bFROM\s+([\"#_\w]+\.[\"\w_#]+)(?:\s\w+,\s+)?([\"#_\w]+\.[\"\w#_]+)?'
                                sf_db_names = []
                                get_main_table_regex = r"\bFROM\s+(?:(?:[\"#_\w]+\.[\"\w_#]+)(?:[\"#\s\w]*,\s+)?)+"
                                matches = re.search(get_main_table_regex, sql_statement, re.IGNORECASE)
                                if matches:
                                    group = matches.group()
                                    for item in group.split(","):
                                        source_table_name = item.replace("FROM", "").strip().split()[0]
                                        updated_source_table_name = re.sub(r'["]+', '', source_table_name)
                                        updated_source_table_name = re.sub(r'[^.\w-]+', '_', updated_source_table_name)
                                        replace_value = read_dataframe.loc[
                                            updated_source_table_name, 'Snowflake_Table_FQN'] if updated_source_table_name in read_dataframe.index else source_table_name
                                        sf_db_name = replace_value.split(".")[0]
                                        if sf_db_name not in sf_db_names:
                                            sf_db_names.append(sf_db_name)

                                final_sf_db_name = None
                                if len(sf_db_names) > 0:
                                    for sf_db_name in sf_db_names:
                                        if sf_db_name.upper().endswith("_DB") or "_PROCS_" in sf_db_name.upper():
                                            final_sf_db_name = sf_db_name
                                            break
                                    else:
                                        final_sf_db_name = sf_db_names[0]
                                db_stage_name = db_stage_mappings.get(final_sf_db_name, None)
                                if db_stage_name is not None:
                                    temp_table_stage_mapping[temp_table_name] = db_stage_name

                        pipeline_grp_param_list = []
                        sql_query_num_mappings = {}
                        param_list_to_process = []
                        # Set default env details
                        if not dry_run:
                            environment_id = next(iter(environment_details_with_ids))
                            snowflake_warehouse = environment_details_with_ids[environment_id].get(
                                'snowflake_warehouse')
                            snowflake_profile = environment_details_with_ids[environment_id].get('snowflake_profile')
                        for i, comment_with_sql in enumerate(parsed_items):
                            if write_dataframe is not None and read_dataframe is not None:
                                modified_sql, env_name, snowflake_profile_new, snowflake_warehouse_new = get_modified_query(
                                    logger, comment_with_sql[-1], parameter_values)
                                comment_with_sql[-1] = modified_sql
                                if snowflake_warehouse_new is not None:
                                    snowflake_warehouse = snowflake_warehouse_new
                                snowflake_profile = snowflake_profile_new
                                if env_name is not None and not dry_run:
                                    env_response = iwx_client.get_environment_id_from_name(env_name)
                                    if env_response["result"]["response"]["environment_id"] is not None:
                                        environment_id = env_response["result"]["response"]["environment_id"]
                                        logger.info(f"Got environment id {environment_id} for {env_name}")
                                    else:
                                        logger.error(f"Unable to get environment id for {env_name}")
                                        logger.error(str(env_response))

                            logger.info(
                                f"Putting this to job queue: {filename} -- SQL Number: {i}. Pipeline will be created in {environment_id} with profile {snowflake_profile} and warehouse {snowflake_warehouse}")
                            params = {
                                'query_position': i,
                                'sql': comment_with_sql[-1],
                                'comment': comment_with_sql[0],
                                'sql_query_num_mappings': sql_query_num_mappings,
                                'dry_run': dry_run,
                                'iwx_client': iwx_client,
                                'environment_id': environment_id,
                                'domain_id': domain_id,
                                'snowflake_profile': snowflake_profile,
                                'snowflake_warehouse': snowflake_warehouse,
                                'pipeline_prefix': pipeline_prefix,
                                'parameter_values': parameter_values,
                                'temp_table_stage_mapping': temp_table_stage_mapping,
                                'database_name': database_name,
                                'create_or_overwrite_pipeline_version': create_or_overwrite_pipeline_version,
                                'jcl_name': jcl_name,
                                'logger': logger,
                                'filename': filename,
                                'file_full_path': file_full_path,
                                'tgt_table_name_list': tgt_table_name_list,
                                'pipeline_grp_param_list': pipeline_grp_param_list,
                                'pipelines_created': pipelines_created,
                                'basic_report_output': basic_report_output
                            }

                            param_list_to_process.append(params)

                        with ThreadPoolExecutor(max_workers=num_fetch_threads) as executor:
                            executor.map(thread_callable, param_list_to_process)
                            executor.shutdown(wait=True)
                        logger.info(f"All the SQLs part of {file_full_path} has been processed successfully")

                        if dry_run:
                            file_to_write_schema_mapped = file_full_path.replace('Output',
                                                                                 'Output_Schema_Mapped').replace('.py',
                                                                                                                 '.sql')
                            try:
                                os.makedirs(os.path.dirname(file_to_write_schema_mapped))
                            except:
                                pass

                            od = collections.OrderedDict(sorted(sql_query_num_mappings.items()))
                            with open(file_to_write_schema_mapped, 'w') as f:
                                for k, sql in od.items():
                                    f.write(
                                        f"---------------------------------------------------------------{k}---------------------------------------------------------\n")
                                    f.write(sql.replace('USE $DB_NAME;', '').strip())
                                    f.write('\n')

                        dry_run_report[file_full_path] = {"number_of_sqls": len(parsed_items),
                                                          "tgt_tables": tgt_table_name_list[::]}
                        # Create or update pipeline group
                        pipeline_group_name = f"{pipeline_group_prefix}-" + re.sub(r'[^\w-]+', '_',
                                                                                   os.path.splitext(filename)[
                                                                                       0].replace('_BTEQ', ''))
                        if not dry_run and len(pipelines_created) > 0:
                            pipelines_created = sorted(pipelines_created, key=lambda x: x[0])
                            env_ids = [pipeline_detail[5] for pipeline_detail in pipelines_created]
                            len_unique_env_ids = len(list(set(env_ids)))
                            if len_unique_env_ids > 1:
                                logger.error(
                                    f"The pipelines in the pipeline group for file {file_full_path} are created in {len_unique_env_ids} different snowflake data environments. Fix this manually!!!")

                                grouped_output = group_by_continuous_env(pipelines_created)
                                for pg_number, group in enumerate(grouped_output):
                                    pipeline_group_id, pipeline_group_id_status = create_iwx_pipeline_group(logger,
                                                                                                            iwx_client,
                                                                                                            pipeline_group_name + f"-{pg_number + 1}",
                                                                                                            group[0][5],
                                                                                                            domain_id,
                                                                                                            group[0][6],
                                                                                                            group[0][7],
                                                                                                            [(i,) + x_gp[1:]
                                                                                                             for i, x_gp
                                                                                                             in
                                                                                                             enumerate(group)])
                                    if pipeline_group_id is not None:
                                        try:
                                            response = iwx_client.modify_advanced_config_for_pipeline_group(domain_id,
                                                                                                            pipeline_group_id,
                                                                                                            adv_config_body={
                                                                                                                "key": "dt_pre_execute_queries",
                                                                                                                "value": f"USE {database_name};",
                                                                                                                "description": "",
                                                                                                                "is_active": True
                                                                                                            },
                                                                                                            action_type="create")
                                            print(response)
                                        except Exception as e:
                                            pass
                                        pg_status_mappings[
                                            pipeline_group_name + f"-{pg_number + 1}"] = pipeline_group_id_status
                                        pipeline_ids_per_env = [gp[1] for gp in group]
                                        temp_pipeline_grp_param_list = [value for item in
                                                                        list(filter(lambda x: list(x.keys())[
                                                                                                  0] in pipeline_ids_per_env,
                                                                                    pipeline_grp_param_list)) for
                                                                        inner_list in item.values() for value in
                                                                        inner_list]
                                        jcl_pipeline_group_mappings.setdefault(jcl_name, []).append(
                                            (pipeline_group_id, pipeline_group_name + f"-{pg_number + 1}",
                                             temp_pipeline_grp_param_list))
                                        for y_gp in group:
                                            pipeline_pipeline_grp_mappings[
                                                y_gp[1]] = pipeline_group_name + f"-{pg_number + 1}"
                            else:
                                pipeline_group_id, pipeline_group_id_status = create_iwx_pipeline_group(logger,
                                                                                                        iwx_client,
                                                                                                        pipeline_group_name,
                                                                                                        environment_id,
                                                                                                        domain_id,
                                                                                                        snowflake_profile,
                                                                                                        snowflake_warehouse,
                                                                                                        pipelines_created)
                                if pipeline_group_id is not None:
                                    try:
                                        response = iwx_client.modify_advanced_config_for_pipeline_group(domain_id,
                                                                                                        pipeline_group_id,
                                                                                                        adv_config_body={
                                                                                                            "key": "dt_pre_execute_queries",
                                                                                                            "value": f"USE {database_name};",
                                                                                                            "description": "",
                                                                                                            "is_active": True
                                                                                                        },
                                                                                                        action_type="create")
                                        print(response)
                                    except Exception as e:
                                        pass
                                    pg_status_mappings[pipeline_group_name] = pipeline_group_id_status
                                    jcl_pipeline_group_mappings.setdefault(jcl_name, []).append(
                                        (pipeline_group_id, pipeline_group_name, pipeline_grp_param_list))
                                    for items in pipelines_created:
                                        pipeline_pipeline_grp_mappings[items[1]] = pipeline_group_name

                except Exception as e:
                    logging.exception(f"Parsing of file {filename} failed due to: " + str(e))
                    traceback.print_exc()
                    basic_report_output.append({"SNOWSQL_FILE_NAME": file_full_path, "SQL_QUERY_ID": None,
                                                "QUERY_NUMBER": query_number, "JCL_NAME": jcl_name,
                                                "PIPELINE_ID": None, 'ENVIRONMENT_ID': None,
                                                'SNOWFLAKE_WAREHOUSE': None, "TARGET_TABLE_NAME": None,
                                                "PARSING_STATUS": "FAILED"})

        for jclname in jcl_pipeline_group_mappings:
            try:
                workflow_name = f"{workflow_prefix}-{jclname}"
                check_wf_exists_response = iwx_client.get_workflow_id(workflow_name, domain_id)
                if check_wf_exists_response["result"]["status"].upper() == "SUCCESS":
                    workflow_id = check_wf_exists_response["result"]["response"]["id"]
                    if workflow_id is not None:
                        for pg_id, pg_name, pg_params in jcl_pipeline_group_mappings[jclname]:
                            pg_wf_mappings[pg_id] = workflow_id
            except Exception as e:
                logger.error(str(e))
                pass

        if not dry_run:
            # writing to csv file
            try:
                df = pd.DataFrame(basic_report_output)
                df['PART_OF_PIPELINE_GROUP'] = df['PIPELINE_ID'].map(pipeline_pipeline_grp_mappings)
                df['PIPELINE_GROUP_STATUS'] = df['PART_OF_PIPELINE_GROUP'].map(pg_status_mappings)
                df['PART_OF_WORKFLOW'] = df['PART_OF_PIPELINE_GROUP'].map(pg_wf_mappings)
                df.to_csv('parsing_report.csv', index=False)
            except Exception as e:
                logger.error("Unable to dump the report: " + str(e))
                traceback.print_exc()
        else:
            # This section is to print basic parsing results when dry_run is set to true
            total_pipelines_to_be_created = 0
            pipeline_groups_pipeline_count = {}
            for item in dry_run_report:
                logger.info(f"File Name: {item}")
                logger.info(f"Number of valid SQL queries parsed: {dry_run_report[item]['number_of_sqls']}")
                total_pipelines_to_be_created = total_pipelines_to_be_created + dry_run_report[item]['number_of_sqls']
                pipeline_to_be_created = []
                for pl_count, tgt_table_name in enumerate(dry_run_report[item]['tgt_tables']):
                    if tgt_table_name is None:
                        random_uuid = uuid.uuid4()
                        tgt_table_name = f"iwx_tgt_table_{random_uuid}"
                    else:
                        params_in_tgt_table_name = re.findall(r'{(.*?)}', tgt_table_name, re.DOTALL)
                        # params_in_tgt_table_name = [group[0] or group[1] for group in params_in_tgt_table_name]
                        if len(params_in_tgt_table_name) > 0:
                            tgt_table_name = re.sub(r"\{([A-Za-z_0-9\s]+)\}", r"\1", tgt_table_name)
                        tgt_table_name = tgt_table_name.split(".")[-1]
                    pipeline_suffix = f"{os.path.splitext(os.path.basename(item))[0].replace('_BTEQ', '')}_{tgt_table_name.upper()}__{pl_count + 1}"
                    pipeline_name = f"{pipeline_prefix}-{pipeline_suffix}"
                    pipeline_name = re.sub(r'[^#\w-]+', '_', pipeline_name)
                    pipeline_to_be_created.append(pipeline_name)
                logger.info(f"Pipelines that will be created are: {', '.join(pipeline_to_be_created)}")
                pipeline_group_name = f"{pipeline_group_prefix}-" + re.sub(r'[^\w-]+', '_',
                                                                           os.path.splitext(os.path.basename(item))[
                                                                               0]).replace('_BTEQ', '')
                pipeline_groups_pipeline_count[pipeline_group_name] = dry_run_report[item]['number_of_sqls']
                logger.info(f"Pipeline group that will be created is: {pipeline_group_name}")

            logger.info(f"Total Pipelines to be Created are: {total_pipelines_to_be_created}")
            logger.info(f"Total Pipeline Groups to be Created are: {len(pipeline_groups_pipeline_count)}")
            for pg in pipeline_groups_pipeline_count:
                logger.info(f"Total Pipelines under {pg} to be created are: {pipeline_groups_pipeline_count[pg]}")

        df_skipped = pd.DataFrame(skipped_sql_queries)
        df_skipped.to_csv('skipped_sql_report.csv', index=False)

        df_deps = pd.DataFrame(dependency_analysis_output)
        df_deps.to_csv('./dependency_analysis/dependency_report.csv', index=False)

    except Exception as e:
        logging.exception("Script failed: " + str(e))
        traceback.print_exc()


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    elapsed_time = end_time - start_time
    # Print the elapsed time
    print(f"Script execution time: {elapsed_time} seconds")
