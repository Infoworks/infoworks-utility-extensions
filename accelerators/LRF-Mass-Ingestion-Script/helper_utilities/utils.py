import json
import os
import re
import pandas as pd
from colorama import Fore, Style
from infoworks.sdk.client import InfoworksClientSDK
import datetime

def print_success(message):
    print(Fore.GREEN + message)
    print(Style.RESET_ALL)


def print_error(message):
    print(Fore.RED + message)
    print(Style.RESET_ALL)


def print_warning(message):
    print(Fore.YELLOW + message)
    print(Style.RESET_ALL)


def print_info(message):
    print(Fore.CYAN + message)
    print(Style.RESET_ALL)


def validate_lrf_layout(lrf_layout):
    lrf_layout = lrf_layout.split(",")
    try:
        for i in lrf_layout:
            temp = int(i.strip())
        return True
    except:
        return False


def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def is_int(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


def trim_spaces(item):
    return item.strip()


def def_val():
    return []


def get_table_id(tables, table_name):
    for item in tables:
        if item['table'].lower() == table_name.lower():
            return str(item['_id'])
    print_error(f"Table Not found {table_name}")


def set_src_advanced_config(obj: InfoworksClientSDK, source_id, adv_config_body=None):
    advance_configs_already_present = []
    client_response = obj.get_advanced_configuration_of_sources(source_id)
    if client_response.get("result")["status"] == "success":
        advance_configs_already_present = client_response.get("result")["response"]

    if adv_config_body is None:
        for item in [("fill_values_for_extra_columns", "true"), ("add_default_audit_columns_to_target", "false"),
                     ("run_first_job_as_cdc", "true"),
                     ("sftp_session_configs", "kex=diffie-hellman-group-exchange-sha256"),
                     ("file_picking_strategy", "first"), ("should_register_hive_udfs", "true")]:
            advance_config_body = {"key": item[0], "value": str(item[-1]), "is_active": True}
            if item[0] in advance_configs_already_present:
                obj.update_source_advanced_configuration(source_id, item[0], advance_config_body)
            else:
                obj.add_source_advanced_configuration(source_id, advance_config_body)
    else:
        key = adv_config_body[0]
        if key in advance_configs_already_present:
            obj.update_source_advanced_configuration(source_id, key, adv_config_body[1])
        else:
            obj.add_source_advanced_configuration(source_id, adv_config_body[1])


def create_src_extensions(obj: InfoworksClientSDK):
    with open("../conf/source_extension_configuration.json", "r") as f:
        source_extension_body = json.loads(f.read())

    obj.create_source_extension(source_extension_body)
    #package_name = source_extension_body["package_name"]
    #source_extension_body.pop("package_name")
    function_alias = []
    #for item in source_extension_body["transformations"]:
    #    function_alias.append(f"{item['alias']}:{package_name}.{item['alias']}")
    #udfs_functions = ";".join(function_alias)
    #advance_config_body = {"key": "hive_udfs_to_register", "value": udfs_functions, "is_active": True}
    return "hive_udfs_to_register", ""


def dump_table_details(obj: InfoworksClientSDK, source_id, table_names_list, column_to_compare, configuration_json,
                       table_schema_df, skipped_tables):
    # get configuration of each table and dump it as csv in tables_metadata.csv file
    tables_list = []
    client_response = obj.list_tables_in_source(source_id)
    if client_response.get("result")["status"] == "success":
        tables_list = client_response.get("result",{}).get("response",{}).get("result",[])
    final_metadata_list = []
    for table in tables_list:
        try:
            #if table["name"] not in table_names_list:
            #    continue
            table_object = {}
            included_columns = [item["name"] for item in table.get("columns",[]) if
                                not item.get("is_excluded_column", False)]
            table_object["IWX_TABLE_ID"] = table["id"]
            table_object["IWX_SOURCE_ID"] = table["source"]
            table_object["TABLE_NAME"] = table["name"]
            table_object["DATALAKE_SCHEMA"] = table["data_lake_schema"]
            table_object["DATALAKE_PATH"] = table["data_lake_path"]
            table_object["TARGET_RELATIVE_PATH"] = table["configuration"]["target_relative_path"]
            table_object["DATALAKE_TABLE_NAME"] = table["configuration"]["deltaLake_table_name"]
            table_object["SOURCE_FILE_TYPE"] = table["configuration"]["source_file_type"]
            table_object["INGEST_SUBDIRECTORIES"] = table["configuration"]["ingest_subdirectories"]
            table_object["SOURCE_RELATIVE_PATH"] = table["configuration"]["source_relative_path"]
            table_object["EXCLUDE_FILENAME_REGEX"] = table["configuration"]["exclude_filename_regex"]
            table_object["INCLUDE_FILENAME_REGEX"] = table["configuration"]["include_filename_regex"]
            table_object["IS_ARCHIVE_ENABLED"] = table["configuration"]["is_archive_enabled"]
            table_object["ENCODING"] = table["configuration"]["source_file_properties"]["encoding"]
            table_object["ESCAPE_CHARACTER"] = table["configuration"]["source_file_properties"]["escape_character"]
            table_object["HEADER_ROWS_COUNT"] = table["configuration"]["source_file_properties"][
                "header_rows_count"]
            columns = table.get("columns",[])
            column_names = [column["original_name"] for column in columns]
            natural_key_regex_from_config = configuration_json["natural_key_regex"]
            table_name = table["name"]
            NATURAL_KEYS = []
            try:
                NATURAL_KEYS = \
                    table_schema_df.query(f"{column_to_compare} == '{table_name}'").fillna('')[
                        'NATURAL_KEYS'].tolist()[
                        0]
                NATURAL_KEYS = json.loads(NATURAL_KEYS)
            except:
                pass
            if len(NATURAL_KEYS) > 0:
                probable_natural_keys = ",".join(NATURAL_KEYS)
            else:
                probable_natural_keys = [column["name"] for column in columns \
                                         if (column["name"].lower().endswith("id") or
                                             column["name"].lower().endswith("key") or
                                             bool(re.match(natural_key_regex_from_config, column["name"])))]
                probable_natural_keys = [col for col in probable_natural_keys if col in included_columns]
                probable_natural_keys = ','.join(probable_natural_keys)
            try:
                natural_keys_from_user =  \
                    table_schema_df.query(f"{column_to_compare} == '{table_name}'").fillna('')[
                        'PROBABLE_NATURAL_KEYS'].tolist()[
                        0]
                natural_keys_from_user = json.loads(natural_keys_from_user)
                if len(natural_keys_from_user)>0:
                    natural_keys_from_user = ",".join(natural_keys_from_user)
                    table_object["PROBABLE_NATURAL_KEYS"] = natural_keys_from_user
            except (IndexError,KeyError) as e:
                table_object["PROBABLE_NATURAL_KEYS"] = probable_natural_keys
            print_info(f"probable_natural_keys for {table['name']}: {probable_natural_keys}")
            merge_columns = [column for column in column_names if
                             column in configuration_json["merge_water_marks_columns"]]
            append_columns = [column for column in column_names if
                              column in configuration_json["append_water_marks_columns"]]
            try:
                storage_format_from_user = table_schema_df.query(f"{column_to_compare} == '{table_name}'").fillna('')[
                        'STORAGE_FORMAT'].tolist()[
                        0]
                if storage_format_from_user!="":
                    table_object["STORAGE_FORMAT"] = storage_format_from_user
                else:
                    table_object["STORAGE_FORMAT"] = configuration_json.get("ingestion_storage_format",
                                                                    table.get("configuration",{}).get("storage_format","parquet"))
            except (IndexError,KeyError) as e:
                table_object["STORAGE_FORMAT"] = configuration_json.get("ingestion_storage_format",
                                                                    table.get("configuration",{}).get("storage_format","parquet"))
            sync_type = "FULL_REFRESH"
            watermark_column = None
            if merge_columns and len(probable_natural_keys.split(",")) != 0:
                sync_type = "INCREMENTAL_MERGE"
                watermark_column = merge_columns[0]
            elif append_columns and len(probable_natural_keys.split(",")) != 0:
                sync_type = "INCREMENTAL_APPEND"
                watermark_column = append_columns[0]
            try:
                ingestion_strategy_from_user = table_schema_df.query(f"{column_to_compare} == '{table_name}'").fillna('')[
                    'INGESTION_STRATEGY'].tolist()[
                    0]
                if ingestion_strategy_from_user != "":
                    table_object["INGESTION_STRATEGY"] = ingestion_strategy_from_user
            except (IndexError,KeyError) as e:
                table_object["INGESTION_STRATEGY"] = sync_type
            table_object["WATERMARK_COLUMN"] = ""
            try:
                watermark_column_from_user = table_schema_df.query(f"{column_to_compare} == '{table_name}'").fillna('')[
                    'WATERMARK_COLUMN'].tolist()[0]
                if watermark_column_from_user != "":
                    table_object["WATERMARK_COLUMN"] = watermark_column_from_user
            except (IndexError,KeyError) as e:
                if watermark_column:
                    table_object["WATERMARK_COLUMN"] = watermark_column

            try:
                scd_type_bool_from_user = table_schema_df.query(f"{column_to_compare} == '{table_name}'").fillna('')[
                    'SCD_TYPE_2'].tolist()[
                    0]
                if scd_type_bool_from_user != "":
                    table_object["SCD_TYPE_2"] = scd_type_bool_from_user
            except (IndexError, KeyError) as e:
                table_object["SCD_TYPE_2"] = 'FALSE'

            try:
                target_database_name_from_user = table_schema_df.query(f"{column_to_compare} == '{table_name}'").fillna('')[
                    'TARGET_DATABASE_NAME'].tolist()[
                    0]
                if target_database_name_from_user != "":
                    table_object["TARGET_DATABASE_NAME"] = target_database_name_from_user
            except (IndexError, KeyError) as e:
                table_object["TARGET_DATABASE_NAME"] = table["configuration"]["target_database_name"]

            try:
                target_schema_name_from_user = table_schema_df.query(f"{column_to_compare} == '{table_name}'").fillna('')[
                    'TARGET_DATABASE_NAME'].tolist()[
                    0]
                if target_schema_name_from_user != "":
                    table_object["TARGET_SCHEMA_NAME"] = target_schema_name_from_user
            except (IndexError, KeyError) as e:
                table_object["TARGET_SCHEMA_NAME"] = table["configuration"]["target_schema_name"]

            try:
                target_target_name_from_user = table_schema_df.query(f"{column_to_compare} == '{table_name}'").fillna('')[
                    'TARGET_TABLE_NAME'].tolist()[
                    0]
                if target_target_name_from_user != "":
                    table_object["TARGET_TABLE_NAME"] = target_target_name_from_user
            except (IndexError, KeyError) as e:
                table_object["TARGET_TABLE_NAME"] = table["configuration"]["target_table_name"]

            table_object["TABLE_GROUP_NAME"] = ''
            table_object["PARTITION_COLUMN"] = json.dumps(
                {"PARTITION_COLUMN": "", "DERIVED_PARTITION": "False", "DERIVED_FORMAT": ""})
            table_object["USER_MANAGED_TABLE"] = 'TRUE'
            user_max_modified_timestamp = configuration_json.get("max_modified_timestamp","")
            format_validate_bool=False
            if user_max_modified_timestamp != "":
                try:
                    format_validate_bool = bool(datetime.datetime.strptime(user_max_modified_timestamp, "%Y-%m-%dT%H:%M:%S.000Z"))
                except ValueError as e:
                    print_error("The format for the max_modified_timestamp should be %Y-%m-%dT%H:%M:%S.000Z.Exiting...")
                    print_error(str(e))
                    exit(-100)
            max_modified_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z") if format_validate_bool == False else user_max_modified_timestamp
            try:
                max_modified_timestamp_from_user = \
                table_schema_df.query(f"{column_to_compare} == '{table_name}'").fillna('')[
                    'MAX_MODIFIED_TIMESTAMP'].tolist()[
                    0]
                if max_modified_timestamp_from_user != "":
                    table_object["MAX_MODIFIED_TIMESTAMP"] = max_modified_timestamp_from_user
            except (IndexError,KeyError) as e:
                table_object["MAX_MODIFIED_TIMESTAMP"] = max_modified_timestamp
            final_metadata_list.append(table_object)
        except Exception as e:
            skipped_tables.append((table['name'], "Configuration of table failed" + str(e), None, None))
            print_error(f"Failed to dump the configuration for {table['name']}")

    if len(final_metadata_list) > 0:
        final_df = pd.DataFrame.from_dict(final_metadata_list)
        final_df = final_df[
            ['IWX_TABLE_ID', 'IWX_SOURCE_ID', 'TABLE_NAME', 'DATALAKE_SCHEMA', 'DATALAKE_PATH',
             'TARGET_RELATIVE_PATH',
             'DATALAKE_TABLE_NAME', 'SOURCE_FILE_TYPE', 'INGEST_SUBDIRECTORIES',
             'SOURCE_RELATIVE_PATH', 'EXCLUDE_FILENAME_REGEX', 'INCLUDE_FILENAME_REGEX', 'IS_ARCHIVE_ENABLED',
             'ENCODING', 'ESCAPE_CHARACTER', 'HEADER_ROWS_COUNT', 'PROBABLE_NATURAL_KEYS', 'STORAGE_FORMAT',
             'INGESTION_STRATEGY', 'WATERMARK_COLUMN', 'SCD_TYPE_2', 'TARGET_DATABASE_NAME', 'TARGET_SCHEMA_NAME',
             'TARGET_TABLE_NAME', 'TABLE_GROUP_NAME', 'PARTITION_COLUMN',
             'USER_MANAGED_TABLE','MAX_MODIFIED_TIMESTAMP']]

        cwd = os.getcwd()
        print_info(f"Dumping the dataframe into {cwd}/tables_metadata.csv")
        final_df.to_csv(f"{cwd}/tables_metadata.csv", index=False)
        print_success(f"Tables metadata CSV is available here : {cwd}/tables_metadata.csv")
    else:
        print_error("final_metadata_list is empty. No tables configured.")

    if len(skipped_tables) > 0:
        cwd = os.getcwd()
        # write skipped tables to a file for later debug
        skipped_tables_df = pd.DataFrame(skipped_tables,
                                         columns=["table_name", "Error", "data_file_columns",
                                                  "columns_from_schema"])
        skipped_tables_df.to_csv('./skipped_tables.csv', index=False)
        print_success(f"Find skipped and failed tables here:{cwd}/skipped_tables.csv")
