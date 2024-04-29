"""
Usage: python bulk_table_configure_with_watermark.py --source_id <your_source_id> --refresh_token
<your_refresh_token> --configure <all/table/tg/workflow> --domain_id <domain_id_where_workflow_will_be_created>
--source_type oracle/teradata/vertica
"""
import re
import sys
import json
import os
from os.path import exists
import pandas as pd
import argparse
from collections import defaultdict
import urllib3
import logging
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.client import InfoworksClientSDK
import infoworks.sdk.local_configurations

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def configure_logger(folder, filename):
    log_level = 'INFO'
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


def trim_spaces(item):
    return item.strip()


def create_domain_if_not_exists(domain_name):
    try:
        get_domain_id_response = iwx_client.get_domain_id(domain_name=domain_name)
        logger.debug(f"get_domain_id() - Response: {json.dumps(get_domain_id_response)}")
        domain_id = get_domain_id_response['result'].get('response', {}).get('domain_id')
        if domain_id is None:
            logger.info(f"Failed to find domain ID for {domain_name}. Creating a new domain")

            list_users_response = iwx_client.list_users({'filter': {"refreshToken": refresh_token}})
            logger.debug(f"list_users() - Response: {json.dumps(list_users_response)}")

            user_info = list_users_response['result']['response'].get('result')
            if user_info:
                logger.info("Found user details")
                user_id = user_info[0]["id"]
            else:
                raise Exception(f"Failed to find user details")

            create_domain_body = {"name": domain_name, "environment_ids": [environment_id],
                                  "users": [user_id]}
            create_domain_response = iwx_client.create_domain(config_body=create_domain_body)
            logger.debug(f"create_domain() - Response: {json.dumps(create_domain_response)}")
            domain_id = create_domain_response['result']['response'].get('result', {}).get("id")
            if domain_id:
                logger.info(f"Created domain successfully: {domain_id}")
            else:
                raise Exception(f"Failed to created domain {domain_name}")
        else:
            logger.info(f"Found domain ID for {domain_name}: {domain_id}")

        return domain_id
    except Exception as error:
        logger.exception(f"Failed to create domain if not exists: {error}")
        sys.exit(-100)


def configure_split_by_for_table(table_name, database_name, columns, column_type_dict, table_payload_dict, source_type,
                                 iwx_column_name_mappings, column_name_case_compatible_dict):
    global configuration_json
    global database_info_df
    try:
        split_by_key_default = database_info_df.query(
            f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'")[
            'SPLIT_BY_KEY_CANDIDATES'].tolist()[0]
    except (KeyError, IndexError) as e:
        logger.info("Skipping Split by configuration as SPLIT_BY_KEY_CANDIDATES was not found in csv")
        logger.info("Error: " + str(e))
        return table_payload_dict
    try:
        derived_split = database_info_df.query(
            f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
            '')['DERIVE_SPLIT_COLUMN_FUCTION'].tolist()[0]
    except (KeyError, IndexError) as e:
        logger.info("Skipping Split by configuration as DERIVE_SPLIT_COLUMN_FUCTION was not found in csv")
        logger.info("Error: " + str(e))
        return table_payload_dict
    if split_by_key_default:
        split_by_key_default = list(map(trim_spaces, split_by_key_default.split(',')))
        split_by_key_default = [column_name_case_compatible_dict[i.upper()] for i in split_by_key_default]
        try:
            split_by_key_default = [iwx_column_name_mappings[i] for i in split_by_key_default]
            split_by_key_default = split_by_key_default[0]
        except KeyError as e:
            logger.error(f"Columns present in this table are : {columns}\n "
                         f"Unknown column provided {str(e)}\n "
                         f"Please validate the same and rerun the script")
            exit(-100)
    else:
        split_by_key_default = ''

    if split_by_key_default and derived_split:
        date_derive_splits = ['dd', 'MM', 'yyyy', 'yyyyMM', 'MMdd', 'yyyyMMdd']
        allowed_values_for_split = ['dd', 'MM', 'yyyy', 'yyyyMM', 'MMdd', 'yyyyMMdd', 'mod',
                                    'hashamp_hashbucket_hashrow']
        derive_func_map = {'dd': "day num in month", 'MM': "month", 'yyyy': "year",
                           'yyyyMM': "year month", 'MMdd': "month day",
                           'yyyyMMdd': "year month day", 'mod': 'mod',
                           'hashamp_hashbucket_hashrow': 'hashamp_hashbucket_hashrow'}
        if derived_split not in allowed_values_for_split:
            logger.error(
                f"derived_split column in CSV should be one of {','.join(allowed_values_for_split)}. "
                f"Unknown property {derived_split} provided")
            return table_payload_dict
        else:
            logger.info(
                f"Deriving the {derive_func_map[derived_split]} from {split_by_key_default} for split by")
            table_payload_dict["split_by_key"] = {
                "parent_column": split_by_key_default.strip(),
                "derive_format": derived_split,
                "is_derived_split": True,
                "split_column": "iwDerivedColumn"
            }
            if derived_split in date_derive_splits:
                table_payload_dict["split_by_key"]["derive_function"] = derive_func_map[derived_split]
            return table_payload_dict
    elif split_by_key_default and derived_split == '':
        logger.info(f"Adding the split by column {split_by_key_default}.")
        table_payload_dict["split_by_key"] = {
            "is_derived_split": False,
            "split_column": split_by_key_default
        }
        return table_payload_dict
    else:
        pass
    logger.info(f"Split By Key Default:{split_by_key_default}")
    split_by_key_override = configuration_json.get('split_by_keys', '')
    split_by_key_override = iwx_column_name_mappings.get(split_by_key_override.strip(), '')
    if split_by_key_override != '' and split_by_key_override in columns and \
            split_by_key_override not in split_by_key_default:
        logger.info(f"Adding split by key as {split_by_key_override}")
        table_payload_dict["split_by_key"] = {
            "is_derived_split": False,
            "split_column": split_by_key_override
        }

    elif split_by_key_default and split_by_key_default in columns and \
            column_type_dict[split_by_key_default[0]] not in [91, 93]:
        logger.info(f"Adding split by key as : {split_by_key_default}")
        table_payload_dict["split_by_key"] = {
            "is_derived_split": False,
            "split_column": split_by_key_default
        }

    elif split_by_key_override in split_by_key_default and len(split_by_key_default) != 0:
        if source_type == 'vertica':
            logger.info(f"Adding split by key as {split_by_key_override}")
            table_payload_dict["split_by_key"] = {
                "is_derived_split": False,
                "split_column": split_by_key_override
            }
        else:
            logger.info(f"Adding split by key as month derived from {split_by_key_override}")
            table_payload_dict["split_by_key"] = {
                "parent_column": split_by_key_override.strip(),
                "derive_format": "MM",
                "derive_function": "month",
                "is_derived_split": True,
                "split_column": "iwDerivedColumn"
            }
    elif (split_by_key_override == '' or split_by_key_override not in columns) and len(split_by_key_default) != 0:
        if source_type == 'vertica':
            logger.info(f"Adding split by key as {split_by_key_default}")
            table_payload_dict["split_by_key"] = {
                "is_derived_split": False,
                "split_column": split_by_key_default
            }
        else:
            logger.info(f"Adding split by key as month derived from {split_by_key_default}")
            table_payload_dict["split_by_key"] = {
                "parent_column": split_by_key_default,
                "derive_format": "MM",
                "derive_function": "month",
                "is_derived_split": True,
                "split_column": "iwDerivedColumn"
            }
    else:
        table_payload_dict["split_by_key"] = None
    return table_payload_dict


def configure_partition_for_table(table_name, database_name, columns, column_type_dict, table_payload_dict,
                                  iwx_column_name_mappings, column_name_case_compatible_dict):
    global configuration_json
    global database_info_df
    try:
        try:
            partition_column = database_info_df.query(
                f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                '')['PARTITION_COLUMN'].tolist()[0].strip()
            if partition_column == "":
                logger.info("Partition column in CSV is empty.Skipping partition..")
                return table_payload_dict
            partition_column = column_name_case_compatible_dict[partition_column.upper()]
            partition_column = iwx_column_name_mappings[partition_column]
        except (KeyError, IndexError):
            logger.info("PARTITION_COLUMN not found in csv")
            return table_payload_dict
        if partition_column in columns:
            logger.info(f"Adding partition column {partition_column}")
            if column_type_dict[partition_column] not in [91, 93]:
                table_payload_dict["partition_keys"] = [
                    {
                        "partition_column": partition_column,
                        "is_derived_partition": False
                    }]
            else:
                try:
                    derived_partition = str(database_info_df.query(
                        f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                        '')['DERIVED_PARTITION'].tolist()[0]).strip()
                except (KeyError, IndexError):
                    return table_payload_dict
                if eval(derived_partition) not in [True, False]:
                    logger.error(f"derived_partition column in CSV should be either True or False. "
                                 f"Unknown property {derived_partition} provided")
                else:
                    if eval(derived_partition):
                        try:
                            derived_format = database_info_df.query(
                                f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                                '')['DERIVED_FORMAT'].tolist()[0].strip()
                        except (KeyError, IndexError):
                            return table_payload_dict
                        allowed_values_for_date_partition = ['dd', 'MM', 'yyyy', 'yyyyMM', 'MMdd', 'yyyyMMdd']
                        derive_func_map = {'dd': "day num in month", 'MM': "month", 'yyyy': "year",
                                           'yyyyMM': "year month", 'MMdd': "month day",
                                           'yyyyMMdd': "year month day"}
                        if derived_format not in allowed_values_for_date_partition:
                            logger.error(
                                f"derived_format column in CSV should be one of {','.join(allowed_values_for_date_partition)}. "
                                f"Unknown property {derived_format} provided")
                        else:
                            logger.info(
                                f"Deriving the {derive_func_map[derived_format]} from {partition_column} for partition")
                            table_payload_dict["partition_keys"] = [
                                {
                                    "parent_column": partition_column,
                                    "derive_format": derived_format,
                                    "derive_function": derive_func_map[derived_format],
                                    "is_derived_partition": True,
                                    "partition_column": "iw_partition_column"
                                }]
                    else:
                        table_payload_dict["partition_keys"] = [
                            {
                                "partition_column": partition_column,
                                "is_derived_partition": False
                            }]
            logger.info(f"Added partition column {partition_column} successfully!")

        else:
            if partition_column != '':
                logger.error(f"{partition_column} column not found in this table. "
                             f"Columns available in table: {','.join(columns)}\n Skipping partition.")
    except Exception as e:
        logger.exception(f"Did not find the partition column in csv file. Skipping partition\n{str(e)}")

    return table_payload_dict


def configure_ingestion_strategy(table_name, database_name, columns, table_payload_dict,
                                 iwx_column_name_mappings, column_name_case_compatible_dict):
    try:
        case_insensitive_merge_columns = [column_name_case_compatible_dict.get(i.upper(), '') for i in
                                          configuration_json["merge_water_marks_columns"]]
        case_insensitive_merge_columns = [i for i in case_insensitive_merge_columns if i != '']

        case_insensitive_append_columns = [column_name_case_compatible_dict.get(i.upper(), '') for i in
                                           configuration_json["append_water_marks_columns"]]
        case_insensitive_append_columns = [i for i in case_insensitive_append_columns if i != '']

        iwx_compatible_merge_Columns = [iwx_column_name_mappings[i] for i in case_insensitive_merge_columns if
                                        i in iwx_column_name_mappings.keys()]
        iwx_compatible_append_Columns = [iwx_column_name_mappings[i] for i in case_insensitive_append_columns if
                                         i in iwx_column_name_mappings.keys()]
        merge_Columns = [value for value in iwx_compatible_merge_Columns if value in columns]
        append_Columns = [value for value in iwx_compatible_append_Columns if value in columns]
        try:
            ingestion_strategy = database_info_df.query(
                f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                '')['INGESTION_STRATEGY'].tolist()[0].strip()
        except (IndexError, KeyError):
            logger.warning("Did not find the ingestion_strategy in csv file. Defaulting to FULL_REFRESH")
            ingestion_strategy = 'FULL_REFRESH'

        logger.info(f"Ingestion Strategy: {ingestion_strategy}")

        if ingestion_strategy not in ['INCREMENTAL_APPEND', 'INCREMENTAL_MERGE', 'FULL_REFRESH', '']:
            logger.error("Provided invalid option for INGESTION_STRATEGY.\n"
                         "Should be one of the following options:\n"
                         "['INCREMENTAL_APPEND','INCREMENTAL_MERGE','FULL_REFRESH']")
        else:
            if ingestion_strategy == 'INCREMENTAL_MERGE':
                table_payload_dict["sync_type"] = "incremental"
                table_payload_dict["update_strategy"] = "merge"
                scd_type = False
                try:
                    scd_type_val = str(database_info_df.query(
                        f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                        '')['SCD_TYPE_2'].tolist()[0]).strip()
                    if eval(scd_type_val.capitalize()) in [False, True]:
                        scd_type = eval(scd_type_val.capitalize())
                    table_payload_dict["is_scd2_table"] = scd_type
                    if scd_type is False:
                        table_payload_dict["scd_type"] = "SCD_1"
                except (KeyError, IndexError):
                    logger.info(f"Did not find the SCD_TYPE_2 column in csv defaulting to SCD TYPE 1 for merge")

                try:
                    merge_column = database_info_df.query(
                        f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                        '')['WATERMARK_COLUMN'].tolist()[0].strip()
                    user_input_merge_columns = list(map(trim_spaces, merge_column.split(',')))
                    case_user_merge_columns = [column_name_case_compatible_dict[column.upper()]
                                               for column in user_input_merge_columns]
                except IndexError:
                    case_user_merge_columns = []
                    logger.error(f"Did not find the watermark columns in CSV even though "
                                 f"the strategy is INCREMENTAL_MERGE. "
                                 f"Defaulting to first merge Column found: {merge_Columns[0]}")
                    if len(merge_Columns) != 0:
                        case_user_merge_columns = [merge_Columns[0]]
                    else:
                        logger.info("Defaulting to Full Refresh as watermark_column is not available in csv")
                        table_payload_dict["sync_type"] = "full-load"

                try:
                    watermark_columns = []
                    for merge_column in case_user_merge_columns:
                        try:
                            if iwx_column_name_mappings[merge_column] in columns:
                                watermark_columns.append(iwx_column_name_mappings[merge_column])
                        except IndexError:
                            logger.error(f"Columns in IWX tables are {columns},"
                                         f" but found non existing column {merge_column} for merge")
                            raise Exception(f"Unknown column {merge_column} used for merge")
                    if watermark_columns:
                        pass
                    else:
                        watermark_columns.append(merge_Columns[0])
                    logger.info("Configuring table for incremental merge mode")
                    logger.info(f"Setting Merge Mode with watermark Columns : {watermark_columns}")
                    table_payload_dict["watermark_columns"] = watermark_columns
                except IndexError as e:
                    logger.error(f"Failed to configure watermark columns: {e}")
                    raise Exception(f"Failed to configure watermark columns: {e}")
            elif ingestion_strategy == 'INCREMENTAL_APPEND':
                table_payload_dict["sync_type"] = "incremental"
                table_payload_dict["update_strategy"] = "append"
                try:
                    append_column = database_info_df.query(
                        f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                        '')['WATERMARK_COLUMN'].tolist()[0].strip()
                    user_input_append_columns = list(map(trim_spaces, append_column.split(',')))
                    case_user_append_columns = [column_name_case_compatible_dict[column.upper()]
                                                for column in user_input_append_columns]
                except IndexError:
                    case_user_append_columns = []
                    logger.error(f"Did not find the watermark columns in CSV even though the strategy is "
                                 f"INCREMENTAL_APPEND. "
                                 f"Defaulting to first append Column found: {append_Columns[0]}")
                    if len(append_Columns) != 0:
                        case_user_append_columns = [append_Columns[0]]
                    else:
                        logger.info("Defaulting to Full Refresh as watermark_column is not available in csv")
                        table_payload_dict["sync_type"] = "full-load"
                try:
                    watermark_columns = []
                    for append_column in case_user_append_columns:
                        try:
                            if iwx_column_name_mappings[append_column] in columns:
                                watermark_columns.append(iwx_column_name_mappings[append_column])
                        except IndexError:
                            logger.error(f"Columns in IWX tables are {columns},"
                                         f" but found non existing column {append_column} for merge")
                            raise Exception(f"Unknown column {append_column} used for append")
                    if watermark_columns:
                        pass
                    else:
                        watermark_columns.append(append_Columns[0])
                    logger.info("Configuring table for incremental append mode")
                    logger.info(f"Setting Append Mode with watermark Column : {watermark_columns}")
                    table_payload_dict["watermark_columns"] = watermark_columns
                except IndexError as e:
                    logger.error(f"Failed to configure watermark columns: {e}")
                    raise Exception(f"Failed to configure watermark columns: {e}")
            else:
                logger.info("Defaulting to Full Refresh")
                table_payload_dict["sync_type"] = "full-load"
        return table_payload_dict
    except Exception as error:
        logger.exception(f"Failed to configure ingestion properties : {error}")
        sys.exit(-100)


def set_advance_config(key, value, source_id, table_id):
    try:
        logger.info("Configuring table with below advance configurations")
        logger.info(f"{key}:{str(value)}")

        # Check if advance config already present if present update the value else add the config
        advance_config_body = {"key": key, "value": str(value), "description": "", "is_active": True}

        get_advance_config_response = iwx_client.get_list_of_advanced_config_of_table(source_id, table_id)
        logger.debug(f"get_list_of_advanced_config_of_table() - Response: {json.dumps(get_advance_config_response)}")
        existing_advance_configs = []
        if get_advance_config_response['result']['status'] == "success":
            for row in get_advance_config_response['result']['response'].get('result'):
                existing_advance_configs.append(row['key'])
            logger.info(f"Existing advance configs for table {table_id} are {existing_advance_configs}")
        else:
            logger.error("Failed to fetch advanced config")

        if key in existing_advance_configs:
            logger.info("Updating the existing advance config keys with the same name")
            update_advance_config_response = iwx_client.modify_advanced_config_of_table(
                source_id, table_id, advance_config_body, 'update', key)
            logger.debug(f"modify_advanced_config_of_table() - Response: {json.dumps(update_advance_config_response)}")
            if update_advance_config_response['result']['status'] == "success":
                logger.info(f"Successfully updated advance config")
            else:
                logger.info(f"Failed to update advance config")
        else:
            logger.info("Adding new advance config")
            create_advance_config_response = iwx_client.modify_advanced_config_of_table(
                source_id, table_id, advance_config_body, 'create')
            logger.debug(f"modify_advanced_config_of_table() - Response: {json.dumps(create_advance_config_response)}")
            if create_advance_config_response['result']['status'] == "success":
                logger.info(f"Successfully created advance config")
            else:
                logger.info(f"Failed to create advance config")
    except Exception as error:
        logger.exception(f"Failed to configure advance config: {error}")


def configure_table_export(table, export_config, incremental, natural_keys, sync_type, watermark_columns, columns):
    try:
        if export_config == {}:
            return "skipped", f"Skipped export table configurations as export is empty"

        logger.info(f"Configuring the table export with below configs : {export_config}")

        export_object = {}
        export_columns = []
        target_data_connection_id = export_config.get('target_data_connection_id')
        if target_data_connection_id:
            get_data_connection_response = iwx_client.get_data_connection(
                params={"filter": {"_id": target_data_connection_id}})
            logger.debug(f"get_data_connection() - Response: {json.dumps(get_data_connection_response)}")
            target_data_connection_details = get_data_connection_response['result']['response'].get('result')
            if target_data_connection_details:
                target_data_connection_type = target_data_connection_details[0]["sub_type"]
            else:
                raise Exception("Failed to get data connection details")
                return "failed", "Failed to get data connection details"
        else:
            target_data_connection_name = export_config.get('target_data_connection_name')
            if target_data_connection_name:
                get_data_connection_response = iwx_client.get_data_connection(
                    params={"filter": {"name": target_data_connection_name}})
                logger.debug(f"get_data_connection() - Response: {json.dumps(get_data_connection_response)}")
                target_data_connection_details = get_data_connection_response['result']['response'].get('result')
                if target_data_connection_details:
                    target_data_connection_id = target_data_connection_details[0]["id"]
                    target_data_connection_type = target_data_connection_details[0]["sub_type"]
                else:
                    raise Exception("Failed to get data connection details")
                    return "failed", "Failed to get data connection details"
            else:
                raise Exception("target_data_connection_id / target_data_connection_name is required")
                return "failed", "target_data_connection_id / target_data_connection_name is required"

        connection_object = {"data_connection_id": target_data_connection_id}
        target_config_object = export_config.get('target_configuration', {}).copy()
        if target_config_object.get('schema_name', "") == "":
            target_config_object['schema_name'] = table.get('configuration', {}).get('target_schema_name',
                                                                                     table.get('catalog_name', ''))
        target_config_object['table_name'] = table.get('configuration', {}).get('target_table_name',
                                                                                table.get('original_table_name', ''))
        target_config_object['natural_key'] = natural_keys

        for item in columns:
            if not item['name'].lower().startswith("ziw"):
                res = {key: item[key] for key in
                       item.keys() & {'original_name', 'name', 'precision', 'sql_type', 'scale'}}
                export_columns.append(res)

        export_object = {"target_type": target_data_connection_type, "connection": connection_object,
                         "target_configuration": target_config_object, "columns": export_columns}

        if incremental:
            logger.info("Configuring table for incremental export")
            logger.info(f"natural Keys: {natural_keys}")
            logger.info(f"sync_type: {sync_type}")
            logger.info(f"watermark_columns : {watermark_columns}")
            export_object["sync_type"] = sync_type.upper()
        else:
            logger.info("Configuring table for Full Export in Overwrite mode")
            export_object["sync_type"] = 'OVERWRITE'

        logger.info("Final export configurations")
        logger.info(json.dumps(export_object))

        table_export_config_payload = {"name": table["name"], "source": table["source"],
                                       "export_configuration": export_object}

        update_table_configuration_response = iwx_client.update_table_configuration(
            table['source'], table['id'], table_export_config_payload)
        logger.debug(f"update_table_configuration() - Response: {json.dumps(update_table_configuration_response)}")
        if update_table_configuration_response['result']['status'] == "success":
            logger.info(f"Table export configurations updated successfully {table['original_table_name']}")
        else:
            logger.error(f"Failed to configure table {table['original_table_name']} export configurations")
        return update_table_configuration_response['result']['status'], update_table_configuration_response.get(
            "result", {}).get("response", {}).get("message", "")
    except Exception as error:
        logger.exception(f"Failed to export table configurations: {error}")
        return "failed", f"Failed to export table configurations: {error}"


def get_table_id(tables, table_name):
    for item in tables:
        if item['name'].lower() == table_name.lower():
            return str(item['id'])
    logger.error(f"table Not found {table_name}..Exiting..")
    exit(-100)


def configure_table_group(table_group_object, source_id):
    try:
        logger.info("Configuring table group with below configurations")
        logger.info(table_group_object)
        temp = []
        already_added_tables = []
        for table in table_group_object["tables"]:
            if table not in already_added_tables:
                temp.append(table)
                already_added_tables.append(table.get("table_id"))
        table_group_object["tables"] = temp
        table_group_details_response = iwx_client.get_table_group_details(source_id,
                                                                          {'filter': {
                                                                              'name': table_group_object['name']}})
        logger.debug(f"get_table_group_details() - Response: {json.dumps(table_group_details_response)}")
        if table_group_details_response['result']['status'] == "success":
            if table_group_details_response['result']['response'].get('result'):
                table_group_id = table_group_details_response['result']['response']['result'][0].get('id')
            else:
                table_group_id = None
                logger.info("Table Group not found")
        else:
            table_group_id = None
            logger.info("Failed to fetch table group details")

        if table_group_id:
            logger.info(f"Found an existing table group in the source with the same name {table_group_object['name']}, "
                        f"Updating the found table group")
            update_table_group_response = iwx_client.update_table_group(source_id, table_group_id, table_group_object)
            logger.info(f"table_group_object:{table_group_object}")
            logger.debug(f"update_table_group() - Response: {json.dumps(update_table_group_response)}")

            if update_table_group_response['result']['status'] == "success":
                logger.info(f"Table group configurations updated successfully {table_group_object['name']}")
            else:
                logger.error(f"Failed to update table group {table_group_object['name']}")

        else:
            create_table_group_response = iwx_client.create_table_group(source_id, table_group_object)
            logger.debug(f"create_table_group() - Response: {json.dumps(create_table_group_response)}")

            if create_table_group_response['result']['status'] == "success":
                logger.info(f"Table group created successfully {table_group_object['name']}")
            else:
                logger.error(f"Failed to create table group {table_group_object['name']}")
    except Exception as error:
        logger.exception(f"Failed to configure table group :{error}")


def tables_configure(source_id, configure_table_group_bool, source_type):
    try:
        global database_info_df
        global metadata_csv_path
        tables_report_list = []
        tables_response = iwx_client.list_tables_in_source(source_id, params={"limit": 1000, "offset": 0})
        logger.debug(f"list_tables_in_source() - Response: {json.dumps(tables_response)}")
        tables = tables_response['result']['response'].get('result')

        tables_in_metadata_csv = database_info_df["TABLENAME"].unique()
        tables_in_metadata_csv = [table.upper() for table in tables_in_metadata_csv]

        default_connection_quota = 0
        if tables:
            default_connection_quota = 100 // len(tables)
        else:
            logger.error(f"No Tables Found in {source_id}. There should be at least one table for table. Exiting.")
            raise Exception(f"No Tables Found in {source_id}")

        tg_tables = []
        source_connection_details_response = iwx_client.get_source_connection_details(source_id)
        logger.debug(f"get_source_connection_details() - Response: {json.dumps(source_connection_details_response)}")

        if source_connection_details_response['result']['response'].get('result'):
            tpt_source_bool = source_connection_details_response['result']['response'].get('result', {}) \
                .get('enable_ingestion_via_tpt', False)
        else:
            logger.error(f"Failed to fetch source connection details of source id {source_id}")
            tpt_source_bool = False

        tables_should_be_picked_from_config = False
        tg_default_dict = defaultdict(list)

        for table in tables:
            if table['original_table_name'].upper() not in tables_in_metadata_csv:
                logger.info(f"Table not found in metadata CSV. "
                            f"Skipping the table configurations for table {table['original_table_name'].upper()}")
                continue

            table_report = {}
            if source_type in ("snowflake", "sqlserver"):
                table_report["database"] = table['schema_name_at_source']
            else:
                if table.get('catalog_is_database', ''):
                    table_report["database"] = table['catalog_name']
                else:
                    table_report["database"] = table['schema_name_at_source']

           # table_report["database"] = table['catalog_name'] if table.get('catalog_is_database','') else table[
            #    'schema_name_at_source']
            table_report["table"] = table['original_table_name']

            tg_tables.append({"table_id": str(table['id']), "connection_quota": default_connection_quota})

            table_payload_dict = {}

            # Table Configurations
            table_id = str(table['id'])
            table_name = table['original_table_name']
            incremental = False
            sync_type = ''
            watermark_column = ''
            if source_type in ("snowflake", "sqlserver"):
                database_name = table['schema_name_at_source']
            else:
                if table.get('catalog_is_database', ''):
                    database_name = table['catalog_name']
                else:
                    database_name = table['schema_name_at_source']
           # database_name = table['catalog_name'] if table.get('catalog_is_database','') else table['schema_name_at_source']
            columns = []
            column_type_dict = {}
            col_object_array = table.get('columns', [])
            probable_natural_keys = []
            iwx_column_name_mappings = {}
            column_name_case_compatible_dict = {}
            for col_object in table.get('columns', []):
                columns.append(col_object['name'])
                column_type_dict[col_object['name']] = col_object.get('target_sql_type', col_object.get('sql_type', ''))
                iwx_column_name_mappings[col_object['original_name']] = col_object['name']
                column_name_case_compatible_dict[col_object['original_name'].upper()] = col_object['original_name']

            logger.info(f"DatabaseName : {database_name}")
            logger.info(f"TableName : {table['original_table_name']}")
            logger.info(f"Columns : {columns}")

            # Configuring Target Storage Type
            storage_format_user = configuration_json.get("ingestion_storage_format", "parquet").strip().lower()
            try:
                storage_format_user = database_info_df.query(
                    f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                    storage_format_user)['STORAGE_FORMAT'].tolist()[0].strip().lower()
            except IndexError:
                logger.warning(f"Defaulting to storage format {storage_format_user} as STORAGE_FORMAT "
                               f"column was not found in csv")

            storage_format = storage_format_user.lower()
            if storage_format in ["orc", "parquet", "avro", "delta", ""]:
                if storage_format != "":
                    logger.info(f"Using the storage format: {storage_format}")
                    table_payload_dict["storage_format"] = storage_format
                else:
                    pass
            else:
                logger.error("Please provide the valid storage format(orc,parquet,delta or avro)\nExiting..")
                raise Exception("Invalid storage format")

            # Configure Sub Set Filters
            try:
                crawl_filters = database_info_df.query(
                    f"TABLENAME.str.upper() =='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'")[
                    'CRAWL_FILTER_CONDITION'].tolist()[0].strip()
                # print(crawl_filters)
                lowercase_to_column_mapping = {}
                for column in columns:
                    lowercase_to_column_mapping[column.lower()] = column
                # print(lowercase_to_column_mapping)
                if crawl_filters:
                    filters = []
                    result = re.split(r'(\[AND\]|\[and\]|\[OR\]|\[or\])', crawl_filters)
                    parent_condition = None
                    for filter_expression in result:
                        if filter_expression in ('[AND]', '[and]', '[OR]', '[or]'):
                            parent_condition = filter_expression.replace('[', '').replace(']', '').upper()
                        else:
                            result = re.split(r'(<=>|<>|<=|>=|=|<|>|is null|IS NULL'
                                              r'|is not null|IS NOT NULL|like|LIKE|not like|NOT LIKE'
                                              r'|rlike|RLIKE|in|IN|not in|NOT IN)',
                                              filter_expression)
                            # print(result)
                            operand = result[0].strip()
                            if operand.lower() in lowercase_to_column_mapping:
                                operand_column_case = lowercase_to_column_mapping[operand.lower()]
                            else:
                                raise Exception(f"Unknown column {operand} in table")
                            operator = result[1].strip().upper()
                            if operator in ('IS NULL', 'IS NOT NULL'):
                                value = ""
                                filter_value_disabled = True
                            else:
                                value = result[2].strip()
                                filter_value_disabled = False
                            filter_dict = {"filter_column": operand_column_case, "filter_operator": operator,
                                           "filter_value": value,
                                           "combine_condition": parent_condition if parent_condition else "-",
                                           "filter_value_disabled": filter_value_disabled}
                            filters.append(filter_dict)
                    table_payload_dict['crawl_data_filter_enabled'] = True
                    table_payload_dict['crawl_filter_conditions'] = filters
            except (KeyError, IndexError) as error:
                logger.warning(f"CRAWL_FILTER_CONDITION column is not found in CSV."
                               f"Skipping Crawl Filter Condition configuration.")
            except Exception as error:
                logger.error(f"Failed to configure CRAWL_FILTER_CONDITION: {error}")

            # Configuring Natural Keys
            probable_natural_keys = ''
            try:
                probable_natural_keys = database_info_df.query(
                    f"TABLENAME.str.upper() =='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'")[
                    'PROBABLE_NATURAL_KEY_COLUMNS'].tolist()[0].strip()
            except IndexError:
                logger.warning(
                    f"PROBABLE_NATURAL_KEY_COLUMNS column is not found in CSV.Skipping natural keys configuration.")
            if probable_natural_keys:
                probable_natural_keys = list(map(trim_spaces, probable_natural_keys.split(',')))
                try:
                    probable_natural_keys = [column_name_case_compatible_dict[i.upper()] for i in probable_natural_keys]
                    probable_natural_keys = [iwx_column_name_mappings[i] for i in probable_natural_keys]
                except KeyError as e:
                    logger.error(f"Columns present in this table are : {columns}\n "
                                 f"Unknown column provided {str(e)}\n "
                                 f"Please validate the same and rerun the script")
                    raise Exception("Unknown column provided")
            else:
                probable_natural_keys = []

            if probable_natural_keys:
                logger.info(f"Probable Natural Keys found are : {probable_natural_keys}")
                table_payload_dict["natural_keys"] = probable_natural_keys

            # Fetching TPT/JDBC
            tpt_or_jdbc = ''
            try:
                tpt_or_jdbc = database_info_df.query(
                    f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                    '')['TPT_OR_JDBC'].tolist()[0].strip()
            except (KeyError, IndexError):
                # Using Source TPT Setting
                logger.warning(f"TPT_OR_JDBC was not found in csv")

            if tpt_or_jdbc == '':
                tpt_or_jdbc = 'TPT' if tpt_source_bool else 'JDBC'

            if tpt_or_jdbc.upper() not in ['TPT', 'JDBC']:
                logger.error(f"Unknown value {tpt_or_jdbc} provided for TPT_OR_JDBC column")
                raise Exception(f"Unknown value {tpt_or_jdbc} provided for TPT_OR_JDBC column")
            logger.info(f"TPT/JDBC Value: {tpt_or_jdbc}")

            # Configuring SPLIT BY KEY
            if tpt_or_jdbc.upper() == 'JDBC':
                table_payload_dict = configure_split_by_for_table(table_name, database_name, columns, column_type_dict,
                                                                  table_payload_dict, source_type,
                                                                  iwx_column_name_mappings,
                                                                  column_name_case_compatible_dict)
            else:
                logger.info("Skipping split by derived split since its tpt source and"
                            " table is configured for tpt ingestion")
                table_payload_dict["split_by_key"] = {"is_derived_split": False}

            # Configuring Partition Column
            table_payload_dict = configure_partition_for_table(table_name, database_name, columns, column_type_dict,
                                                               table_payload_dict, iwx_column_name_mappings,
                                                               column_name_case_compatible_dict)

            # Configuring Ingestion Strategy
            table_payload_dict = configure_ingestion_strategy(table_name, database_name, columns,
                                                              table_payload_dict, iwx_column_name_mappings,
                                                              column_name_case_compatible_dict)

            # Configuring the tpt advance configs for readers and writers count picked from CSV
            tpt_readers = ''
            tpt_writers = ''

            try:
                tpt_readers = database_info_df.query(
                    f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                    '')['TPT_READER_INSTANCES'].tolist()[0]
                tpt_writers = database_info_df.query(
                    f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                    '')['TPT_WRITER_INSTANCES'].tolist()[0]
            except (IndexError, KeyError):
                pass
            advance_configs_body = {}
            if tpt_readers != '':
                set_advance_config('tpt_reader_instances', int(tpt_readers), source_id, table_id)

            if tpt_writers != '':
                set_advance_config('tpt_writer_instances', int(tpt_writers), source_id, table_id)

            if tpt_source_bool and tpt_or_jdbc.upper() == 'TPT':
                table_payload_dict["data_fetch_type"] = "tpt"
            elif tpt_source_bool and tpt_or_jdbc.upper() == 'JDBC':
                table_payload_dict["data_fetch_type"] = "jdbc"
            else:
                pass
                # table_payload_dict["data_fetch_type"] = "jdbc"

            # set the config for parquet bypass in case of tpt
            if tpt_source_bool and tpt_or_jdbc.upper() == 'TPT' and table_payload_dict['sync_type'] == "full-load":
                set_advance_config('create_hive_compatible_target_table', 'true', source_id, table_id)
                table_payload_dict["data_fetch_type"] = "skip_iw_processing"
                table_payload_dict["storage_format"] = "csv"

            if table_payload_dict.get('update_strategy', 'overwrite') in ["merge", "append"]:
                incremental = True

            sync_type = table_payload_dict.get('update_strategy', 'overwrite')

            watermark_columns = table_payload_dict.get('watermark_columns', '')
            # target table name and target schema name overwrite
            target_table_name = ''
            target_schema_name = ''
            try:
                target_table_name = database_info_df.query(
                    f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                    '')['TARGET_TABLE_NAME'].tolist()[0].strip()
                target_schema_name = database_info_df.query(
                    f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                    '')['TARGET_SCHEMA_NAME'].tolist()[0].strip()
            except (IndexError, KeyError):
                # Using Previous Target Table Configuration
               # target_schema_name = table['target_schema_name']
               # target_table_name = table['target_table_name']
                logger.info("Defaulting to original table name and schema name as target table name "
                            "and schema name as the entry was not found in csv")

            if target_table_name != '':
                logger.info(f"target_table_name : {target_table_name}")
                table_payload_dict["target_table_name"] = target_table_name.replace(" ", "_")
            else:
                target_table_name = table["configuration"]['target_table_name']
                table_payload_dict["target_table_name"] = target_table_name
            if target_schema_name != '':
                logger.info(f"target_schema_name : {target_schema_name}")
                table_payload_dict["target_schema_name"] = target_schema_name.replace(" ", "_")
            else:
                target_schema_name = table["configuration"]['target_schema_name']
                table_payload_dict["target_schema_name"] = target_schema_name
            # Configure Table Type in Target Configuration

            default_table_type = configuration_json.get("default_table_type", "infoworks_managed_table").strip().lower()
            try:
                table_type = database_info_df.query(
                    f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                    storage_format_user)['TABLE_TYPE'].tolist()[0].strip().lower()

                if table_type not in ["infoworks_managed_table", "user_managed_table_without_data",
                                      "user_managed_table_with_data"]:
                    table_type = default_table_type
                    logger.error("Invalid Table Type, Only [infoworks_managed_table, user_managed_table_without_data, "
                                 "user_managed_table_with_data] are supported. Defaulting to *infoworks_managed_table*")
            except (IndexError, KeyError):
                table_type = default_table_type
                logger.warning(f"Defaulting to table type {table_type}")

            # Fetch Data Environment Details
            data_environment_type = None
            try:
                get_environment_details_response = iwx_client.get_environment_details(
                    params={"filter": {"_id": environment_id}})
                logger.debug(f"get_environment_details() - Response: {json.dumps(get_environment_details_response)}")

                if get_environment_details_response['result']['response'].get('result'):
                    environment_details = get_environment_details_response['result']['response']['result'][0]
                    data_environment_type = environment_details.get("data_warehouse_type", "") \
                        if environment_details.get("data_warehouse_type", "") != "" \
                        else environment_details["platform"]
                    logger.info(f"Data Environment Type: {data_environment_type}")
                else:
                    logger.error("Failed to get environment details")
            except Exception as error:
                logger.error(f"Failed to get data environment_details - Error: {error}")

            table_payload_dict["target_table_type"] = table_type
            user_managed_table_path = None
            if table_type == "infoworks_managed_table":
                # Uses Infoworks Target Path
                pass
            else:
                # User Managed Tables
                if data_environment_type:
                    if data_environment_type.lower() in ('snowflake', 'bigquery'):
                        # CDWs don't take a target path
                        pass
                    else:
                        try:
                            user_managed_table_target_path = database_info_df.query(
                                f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                                '')['USER_MANAGED_TABLE_TARGET_PATH'].tolist()[0].strip()

                            get_source_configurations_response = iwx_client.get_source_configurations(source_id)
                            logger.debug(f"get_source_configurations() - "
                                         f"Response: {json.dumps(get_source_configurations_response)}")
                            base_location = ''
                            if get_source_configurations_response['result']['response'].get('result'):
                                base_location = get_source_configurations_response['result']['response']['result'] \
                                    .get('data_lake_path')
                            else:
                                base_location = ''
                                logger.info(f"Failed to get source configurations of source id {source_id}")
                            if user_managed_table_target_path == "":
                                user_managed_table_path = f"{base_location}/{target_schema_name}" \
                                                          f"/{target_table_name}"
                            else:
                                user_managed_table_path = user_managed_table_target_path
                        except (IndexError, KeyError):
                            user_managed_table_path = f"{base_location}/{target_schema_name}" \
                                                      f"/{target_table_name}"
                            logger.info(
                                f"Could not find USER_MANAGED_TABLE_TARGET_PATH in csv. Defaulting to {user_managed_table_path}")
                else:
                    logger.error("Failed to get data environment details, Setting table as infoworks managed")
                    table_payload_dict["target_table_type"] = "infoworks_managed_table"

            skip_iw_process = 'False'
            try:
                skip_iw_process = str(database_info_df.query(
                    f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                    '')['TPT_WITHOUT_IWX_PROCESSING'].tolist()[0]).strip()
            except (IndexError, KeyError):
                logger.info("Skipping TPT_WITHOUT_IWX_PROCESSING as the entry was not found in csv")

            if eval(skip_iw_process):
                set_advance_config('create_hive_compatible_target_table', 'true', source_id, table_id)
                table_payload_dict["data_fetch_type"] = "skip_iw_processing"
                if table_payload_dict["sync_type"] == "incremental":
                    table_payload_dict["update_strategy"] = "append"
                table_payload_dict["storage_format"] = "csv"
                table_payload_dict["split_by_key"] = {"is_derived_split": False}

            if table_payload_dict.get("data_fetch_type") == "skip_iw_processing":
                table_payload_dict["split_by_key"] = {"is_derived_split": False}

            # table group attachment
            self_table_grp_name = ''
            try:
                self_table_grp_name = database_info_df.query(
                    f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                    '')['TABLE_GROUP_NAME'].tolist()[0].strip()
            except (IndexError, KeyError):
                pass
            if self_table_grp_name != '':
                tg_default_dict[self_table_grp_name].append(table_id)

            logger.info(f"Table Config Payload: {json.dumps(table_payload_dict)}")

            final_update_table_payload = {"configuration": table_payload_dict,
                                          "source": source_id,
                                          "name": table_name}
            if user_managed_table_path:
                final_update_table_payload['user_managed_table_path'] = user_managed_table_path

            # exclude audit columns based on exclude_audit_columns param in configurations json
            table_schema = table.get("columns", [])
            if configuration_json.get("exclude_audit_columns", False) == True:
                logger.info("Excluding the audit columns for this table as exclude_audit_columns is set to true")
                for column in table_schema:
                    if column.get("is_audit_column", ""):
                        column["is_excluded_column"] = True
            else:
                for column in table_schema:
                    if column.get("is_audit_column", ""):
                        column["is_excluded_column"] = False

            # add rtrim function to strings if RTRIM_STRING_COLUMNS is True in csv
            apply_rtrim_to_strings=False
            try:
                apply_rtrim_to_strings = str(database_info_df.query(
                    f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                    False)['RTRIM_STRING_COLUMNS'].tolist()[0])
            except (IndexError, KeyError):
                apply_rtrim_to_strings=False
            if eval(apply_rtrim_to_strings):
                for column in table_schema:
                    if column.get("sql_type","")==12:
                        column["transform_mode"] = "advanced"
                        if column.get("transform_derivation","")=="":
                            column["transform_derivation"] = f"nvl(rtrim({column['name']}),'')"

            # configure sort by columns if given for snowflake environment
            sort_by_columns = None
            try:
                sort_by_columns = str(database_info_df.query(
                    f"TABLENAME.str.upper()=='{table_name.upper()}' & DATABASENAME.str.upper()=='{database_name.upper()}'").fillna(
                    False)['SORT_BY_COLUMNS'].tolist()[0])
            except (IndexError, KeyError):
                sort_by_columns = None
            if sort_by_columns:
                sort_by_columns = list(map(trim_spaces, sort_by_columns.split(',')))
                try:
                    sort_by_columns = [column_name_case_compatible_dict[i.upper()] for i in sort_by_columns]
                    sort_by_columns = [iwx_column_name_mappings[i] for i in sort_by_columns]
                except KeyError as e:
                    logger.error(f"Columns present in this table are : {columns}\n "
                                 f"Unknown column provided {str(e)} as SORT_BY_COLUMNS\n "
                                 f"Please validate the same and rerun the script")
                    raise Exception("Unknown column provided")
            else:
                sort_by_columns = []

            if sort_by_columns and data_environment_type.lower() in ["snowflake","bigquery"]:
                logger.info(f"sorting_columns : {sort_by_columns}")
                table_payload_dict["sorting_columns"] = sort_by_columns
            if snowflake_metasync_source_id is not None:
                target_table_name = table_payload_dict["target_table_name"]
                target_schema_name = table_payload_dict["target_schema_name"]
                target_database_name = configuration_json.get("sfDatabase", "")
                snowflake_metasync_source_table_response = iwx_client.list_tables_in_source(source_id=snowflake_metasync_source_id,
                                                                                            params={"filter":{"catalog_name":target_database_name.upper(),
                                                                                                    "schemaNameAtSource":target_schema_name.upper(),
                                                                                                    "origTableName":target_table_name.upper()}})

                snowflake_metasync_source_table = snowflake_metasync_source_table_response["result"]["response"].get("result",[])
                if snowflake_metasync_source_table:
                    teradata_table_column_names = [column["name"].lower() for column in table_schema]
                    snowflake_metasync_source_table=snowflake_metasync_source_table[0]
                    snowflake_metasync_columns = snowflake_metasync_source_table.get("columns",[])
                    index=len(teradata_table_column_names)+1
                    snowflake_user = None
                    snowflake_env_details_response = iwx_client.get_environment_details(environment_id=environment_id)
                    snowflake_env_details = snowflake_env_details_response.get("result",{}).get("response",{}).get("result",{})
                    if snowflake_env_details:
                        snowflake_env_details=snowflake_env_details[0]
                        snowflake_user=snowflake_env_details.get("data_warehouse_configuration",{})\
                        .get("authentication_properties",{})\
                        .get("username","")
                    function_mappings = {
                        "DW_LOAD_DT":"current_date()",
                        "DW_LOAD_TM":"date_format(current_timestamp(), 'HH:mm:ss')",
                        "DW_UPDT_TS":"current_timestamp()",
                        "DW_LOAD_USER":f"\"{snowflake_user.upper()}\"",
                        "DW_UPDT_USER":f"\"{snowflake_user.upper()}\""
                    }
                    snowflake_metasync_source_table_columns = [column["name"].lower() for column in snowflake_metasync_columns]
                    snowflake_metasync_source_table_columns_dict = {}
                    for column in snowflake_metasync_columns:
                        snowflake_metasync_source_table_columns_dict[column["name"].lower()]=column
                    for column in table_schema:
                        #todo if teradata column is not in snowflake exclude them
                        if column["name"].lower() not in snowflake_metasync_source_table_columns and not column.get("is_audit_column", False):
                            column["is_excluded_column"] = True
                        else:
                            # Todo if teradata column is of integer type and snowflake sql type is 92(TIME) or 91(date) or 93(timestamp) then add advance function from_unixtime(columnval,"HH:mm:ss")
                            #bigint = -5
                            #decimal = 3
                            #integer = 4
                            if snowflake_metasync_source_table_columns_dict.get(column["name"].lower(),{}).get("sql_type","")==92 and column["sql_type"] in [-5,3,4]:
                                column["transform_mode"] = "advanced"
                                column["transform_derivation"] = f"from_unixtime({column['name']},'HH:mm:ss')"
                            elif snowflake_metasync_source_table_columns_dict.get(column["name"].lower(),{}).get("sql_type","")==91 and column["sql_type"] in [-5,3,4]:
                                column["transform_mode"] = "advanced"
                                column["transform_derivation"] = f"to_date(from_unixtime({column['name']},'yyyy-MM-dd'),'yyyy-MM-dd')"
                            elif snowflake_metasync_source_table_columns_dict.get(column["name"].lower(),{}).get("sql_type","")==93 and column["sql_type"] in [-5,3,4]:
                                column["transform_mode"] = "advanced"
                                column["transform_derivation"] = f"to_timestamp(from_unixtime({column['name']},'yyyy-MM-dd HH:mm:ss.SSS'),'yyyy-MM-dd HH:mm:ss.SSS')"
                            else:
                                pass
                    #section to add missing columns as per snowflake
                    for column in snowflake_metasync_columns:
                        if column["name"].lower() not in teradata_table_column_names:
                            temp_dict = {"column_type": "target", "sql_type": column.get("sql_type", 12), "is_deleted": False,
                                         "name": column["name"].upper(), "original_name": column["name"].upper(),
                                         "target_column_order":index,
                                         "target_sql_type": column.get("target_sql_type", 12),
                                         "is_audit_column": False}
                            index+=1
                            if temp_dict["target_sql_type"] == 91:
                                temp_dict["format"] = "yyyy-MM-dd"
                            if temp_dict["target_sql_type"] == 93:
                                temp_dict["format"] = "yyyy-MM-dd HH:mm:ss.SSS"
                            if function_mappings.get(column["name"].upper(),""):
                                temp_dict["transform_mode"] = "advanced"
                                temp_dict["transform_derivation"]=function_mappings.get(column["name"].upper(),"")
                            table_schema.append(temp_dict)
                            #print("temp_dict: ",temp_dict)
                else:
                    logger.error(f"Could not find the snowflake table {target_table_name} with database:{target_database_name},schema:{target_schema_name}")
                    logger.error(snowflake_metasync_source_table_response)
                    logger.info("Skipping updating of schema as per snowflake table")

            # update the column data types as per mappings provided in configurations.json
            # column_type_mappings={
            #     "String":12,
            #     "Decimal":3,
            #     "Integer":4,
            #     "Float":7,
            #     "Double":8,
            #     "Date":91,
            #     "Timestamp":93,
            #     "Boolean":16,
            #     "Byte":-2,
            #     "Long":-5
            # }
            # column_type_number_to_name_mappings = {v: k for k, v in column_type_mappings.items()}
            # mappings_from_configuration_json = configuration_json.get("update_column_type_mappings",{})
            # mappings_from_configuration_json = {k.title():v.title() for k,v in mappings_from_configuration_json.items()}
            # if mappings_from_configuration_json!={}:
            #     for column in table_schema:
            #         column_type_name=column_type_number_to_name_mappings.get(column["target_sql_type"],"")
            #         if column_type_name in mappings_from_configuration_json.keys():
            #             #get the mapped column name from configuration json schema lookup
            #             mapped_column_type = mappings_from_configuration_json.get(column_type_name, "")
            #             #only if the mapped_column is available in out column_type_mappings update the schema
            #             if column_type_mappings.get(mapped_column_type,""):
            #                 column["target_sql_type"]=column_type_mappings.get(mapped_column_type,"")
            final_update_table_payload["columns"] = table_schema

            update_table_configuration_response = iwx_client.update_table_configuration(
                source_id, table_id, final_update_table_payload)
            logger.debug(f"configure_table_ingestion_properties_with_payload() - "
                         f"Response: {json.dumps(update_table_configuration_response)}")
            table_report["ingestion_configuration_update_status"] = update_table_configuration_response['result'][
                'status']
            if update_table_configuration_response['result']['status'] == "success":
                table_report[
                    "ingestion_configuration_message"] = f"Table configurations updated successfully {table['original_table_name']}"
                logger.info(f"Table configurations updated successfully {table['original_table_name']}")
                if configuration_json.get("export_configurations", "") != "":
                    if data_environment_type:
                        if data_environment_type.lower() not in ('snowflake', 'bigquery'):
                            status, message = configure_table_export(table, configuration_json["export_configurations"],
                                                                     incremental,
                                                                     probable_natural_keys, sync_type,
                                                                     watermark_columns, col_object_array)
                            table_report["export_configurations_status"] = status
                            table_report["export_configurations_message"] = message
                        else:
                            logger.info("Sync to Target is not available for snowflake/bigquery")
                            table_report["export_configurations_status"] = "skipped"
                            table_report[
                                "export_configurations_message"] = "Sync to Target is not available for snowflake/bigquery"
                    else:
                        logger.error("Failed to get data environment details, Skipping Sync to Target Configuration")
                        table_report["export_configurations_status"] = "skipped"
                        table_report[
                            "export_configurations_message"] = "Failed to get data environment details, Skipping Sync to Target Configuration"
            else:
                logger.error(f"Failed to configure table {table['original_table_name']}")
                table_report[
                    "ingestion_configuration_message"] = f"Failed to configure table {table['original_table_name']}\n{update_table_configuration_response.get('result', {}).get('response', {}).get('message', '')}"
            tables_report_list.append(table_report)
        print(
            f"Writing the table configuration status report to {metadata_csv_path.rstrip('.csv')}_tables_configuration_report.csv")
        tables_configuration_report_df = pd.DataFrame(tables_report_list)
        tables_configuration_report_df.to_csv(f"{metadata_csv_path.rstrip('.csv')}_tables_configuration_report.csv")
        # Table Group Configurations
        if configure_table_group_bool:
            tg_defaults = configuration_json["table_groups"]
            table_group_object = {}

            for table_group in tg_defaults:
                tables_from_config = []
                if tg_default_dict[table_group['name']]:
                    config_connection_quota = 100 // len(tg_default_dict[table_group['name']])
                    override_connection_quota = 0
                    for table in tg_default_dict[table_group['name']]:
                        get_table_details_response = iwx_client.get_table_info(source_id, table)
                        logger.debug(f"get_table_info() - Response: {json.dumps(get_table_details_response)}")
                        table_name_at_source = get_table_details_response.get('original_table_name')
                        schema_name_at_source = get_table_details_response.get('schema_name_at_source')
                        if table_name_at_source and schema_name_at_source:
                            pass
                        else:
                            logger.error("Failed to get table details.")
                        try:
                            override_connection_quota = int(database_info_df.query(
                                f"TABLENAME=='{table_name_at_source}' & DATABASENAME=='{schema_name_at_source}'")
                                                            .fillna(0)['CONNECTION_QUOTA'].tolist()[0])
                        except (KeyError, IndexError):
                            pass
                        if override_connection_quota != 0 and override_connection_quota != '' and table_name_at_source \
                                and schema_name_at_source:
                            tables_from_config.append(
                                {"table_id": table, "connection_quota": override_connection_quota})
                        else:
                            tables_from_config.append({"table_id": table, "connection_quota": config_connection_quota})
                    tables_should_be_picked_from_config = True
                elif len(table_group.get('table_names', [])) != 0:
                    config_connection_quota = 100 // len(table_group.get('table_names', []))
                    tables_from_config = []
                    logger.info(f"Configuring table group {table_group['name']}")
                    for table in table_group.get('table_names', []):
                        tables_from_config.append(
                            {"table_id": get_table_id(tables, table), "connection_quota": config_connection_quota})
                    tables_should_be_picked_from_config = True
                else:
                    tables_from_config = []
                    config_connection_quota = 100 // len(tables)
                    config_connection_quota_max_parallel = 100 // table_group.get('max_parallel_tables', len(tables))
                    config_connection_quota = max(config_connection_quota, config_connection_quota_max_parallel)
                    for table in tables:
                        tables_from_config.append(
                            {"table_id": str(table['id']), "connection_quota": config_connection_quota})
                    tables_should_be_picked_from_config = True

                environment_compute_id_response = iwx_client.get_compute_id_from_name(environment_id,
                                                                                      table_group.get(
                                                                                          'environment_compute_name',
                                                                                          ''))
                logger.debug(f"get_compute_id_from_name() - Response: {json.dumps(environment_compute_id_response)}")
                if environment_compute_id_response['result']['response'].get('compute_id'):
                    environment_compute_id = environment_compute_id_response['result']['response']['compute_id']
                else:
                    logger.error(f"Failed to find environment compute id corresponding to id {environment_id}")
                    raise Exception(f"Failed to find environment compute id corresponding to id {environment_id}")

                table_group_object["environment_compute_template"] = {
                    "environment_compute_template_id": environment_compute_id}
                table_group_object["name"] = table_group["name"]
                table_group_object["max_connections"] = table_group.get('max_connections_to_source', 1)
                table_group_object["max_parallel_entities"] = table_group.get('max_parallel_tables', len(tables))
                if table_group.get("combined_schedule",""):
                    table_group_object["combined_schedule"]=table_group.get("combined_schedule",{})
                if table_group.get("has_schedule_changed", ""):
                    table_group_object["has_schedule_changed"] = table_group.get("has_schedule_changed", True)
                if table_group.get("scheduling", ""):
                    table_group_object["scheduling"] = table_group.get("scheduling", "combined")
                if table_group.get("scheduling", ""):
                    table_group_object["scheduler_username"] = table_group.get("scheduler_username", "")
                if table_group.get("scheduler_username", ""):
                    table_group_object["scheduler_auth_token"] = table_group.get("scheduler_auth_token", "")
                if tables_should_be_picked_from_config:
                    table_group_object["tables"] = tables_from_config
                else:
                    table_group_object["tables"] = tg_tables

                configure_table_group(table_group_object, source_id)
    except Exception as error:
        logger.exception(f"Failed to configure table/table group : {error}")
        sys.exit(-100)


def configure_workflow(source_id, domain_id):
    try:
        global workflow_name_id_mappings
        # Checking if source is accessible to domain
        sources_associated_with_domain_response = iwx_client.get_sources_associated_with_domain(domain_id)
        logger.debug(f"get_sources_associated_with_domain() - "
                     f"Response: {json.dumps(sources_associated_with_domain_response)}")
        if sources_associated_with_domain_response['result']['response'].get('result'):
            accessible_source_ids = []
            for source in sources_associated_with_domain_response['result']['response'].get('result'):
                accessible_source_ids.append(source['id'])

            if source_id in accessible_source_ids:
                logger.info(f"Source {source_id} is accessible by domain")
            else:
                logger.error(f"Source {source_id} is not accessible by the domain {domain_id}."
                             f"Please add the same")
                raise Exception(f"Source {source_id} is not accessible by the domain {domain_id}."
                                f"Please add the same")
        else:
            logger.error(f"No sources found accessible with the domain {domain_id}")
            raise Exception(f"No sources found accessible with the domain {domain_id}")

        source_name_response = iwx_client.get_sourcename_from_id(source_id)
        logger.debug(f"get_sourcename_from_id() - Response: {json.dumps(source_name_response)}")
        if source_name_response['result']['response'].get('name'):
            source_name = source_name_response['result']['response']['name']
        else:
            logger.error(f"Failed to get source name from id {source_id}")
            raise Exception(f"Failed to get source name from id {source_id}")

        ingest_workflow_model = {
            "name": "",
            "description": "",
            "child_workflow_ids": [],
            "workflow_graph": {
                "tasks": [
                    {
                        "is_group": False,
                        "title": "Ingest Source",
                        "description": "",
                        "task_id": "SI_KE7I",
                        "task_type": "ingest_table_group",
                        "location": "-385 -105",
                        "run_properties": {
                            "num_retries": 0,
                            "trigger_rule": "all_success"
                        },
                        "task_properties": {
                            "source_id": "",
                            "table_group_id": "",
                            "ingest_type": "all"
                        }
                    }
                ],
                "edges": []
            }
        }
        ingest_export_workflow_model = {
            "name": "",
            "description": "",
            "child_workflow_ids": [],
            "workflow_graph": {
                "tasks": [
                    {
                        "is_group": False,
                        "title": "Ingest Source",
                        "description": "",
                        "task_id": "SI_KE7I",
                        "task_type": "ingest_table_group",
                        "location": "-385 -105",
                        "run_properties": {
                            "num_retries": 0,
                            "trigger_rule": "all_success"
                        },
                        "task_properties": {
                            "source_id": "",
                            "table_group_id": "",
                            "ingest_type": "all"
                        }
                    },
                    {
                        "is_group": False,
                        "task_type": "export",
                        "task_id": "EX_2CD5",
                        "location": "-255 -105",
                        "title": "Sync to External Target",
                        "description": "",
                        "task_properties": {
                            "export_type": "source",
                            "source_id": "",
                            "table_group_id": "",
                            "pipeline_id": None,
                            "target_table_id": None
                        },
                        "run_properties": {
                            "trigger_rule": "all_success",
                            "num_retries": 0
                        }
                    }
                ],
                "edges": [{
                    "from_task": "SI_KE7I",
                    "to_task": "EX_2CD5",
                    "category": "LINK"
                }]
            }
        }
        if configuration_json.get("export_configurations", {}) != {}:
            workflow_model = ingest_export_workflow_model
        else:
            workflow_model = ingest_workflow_model
        table_groups = configuration_json.get("table_groups", [])
        workflows_list = []
        for tg in table_groups:
            tg_name = tg.get('name', None)
            workflow_name = f"{source_name}_{tg_name}_ingest_export_wf"
            workflow_model["name"] = workflow_name
            if tg_name is None:
                logger.error("Table group section in configurations.json is not found. "
                             "Please validate the same. Exiting.")
                raise Exception("Table group section in configurations.json is not found.")
            table_group_details_response = iwx_client.get_table_group_details(source_id, {'filter': {'name': tg_name}})
            logger.debug(f"get_table_group_details() - Response: {json.dumps(table_group_details_response)}")
            if table_group_details_response['result']['status'] == "success":
                if table_group_details_response['result']['response'].get('result'):
                    table_group_id = table_group_details_response['result']['response']['result'][0].get('id')
                else:
                    table_group_id = None
            else:
                table_group_id = None
                logger.info("Failed to fetch table group details")

            if table_group_id is None:
                logger.error("No Table groups under given source")
                raise Exception("No Table groups under given source")
            else:
                for task in workflow_model["workflow_graph"]["tasks"]:
                    task["task_properties"]["source_id"] = source_id
                    task["task_properties"]["table_group_id"] = table_group_id
                logger.info(f"Configure workflow {workflow_name} with :")
                logger.info(workflow_model)
                get_workflow_response = iwx_client.get_list_of_workflows(domain_id)
                logger.debug(f"get_list_of_workflows() - Response: {json.dumps(get_workflow_response)}")
                if get_workflow_response['result']['status'] == "success":
                    index = None
                    if get_workflow_response['result']['response'].get('result'):
                        for index_val, workflow in enumerate(get_workflow_response['result']['response']['result']):
                            if workflow["name"] == workflow_name:
                                index = index_val
                                break
                        if index is not None:
                            workflow_id = get_workflow_response['result']['response']['result'][index].get('id')
                        else:
                            workflow_id = None
                    else:
                        workflow_id = None
                else:
                    workflow_id = None
                    logger.info("Failed to fetch workflow details")

                if workflow_id:
                    update_workflow_response = iwx_client.update_workflow(workflow_id, domain_id, workflow_model)
                    logger.debug(f"update_workflow() - Response: {json.dumps(update_workflow_response)}")
                    if update_workflow_response['result']['status'] == "success":
                        logger.info(f"Updated workflow {workflow_name} successfully!")
                    else:
                        logger.error(f"Failed to update workflow {workflow_name}")
                else:
                    create_workflow_response = iwx_client.create_workflow(domain_id, workflow_model)
                    logger.debug(f"create_workflow() - Response: {json.dumps(create_workflow_response)}")
                    if create_workflow_response['result']['response'].get('result'):
                        workflow_id = create_workflow_response['result']['response']['result'].get('id')
                        workflow_name_id_mappings[workflow_id] = workflow_name
                    else:
                        workflow_id = None
                        logger.error("Failed to create workflow")

                if workflow_id:
                    workflows_list.append(workflow_id)
                else:
                    logger.error(f"Failed to configure workflow {workflow_name}")

        return workflows_list
    except Exception as error:
        logger.exception(f"Failed to configure workflow : {error}")
        sys.exit(-100)


def run_workflow(workflow_id, domain_id, poll_workflow_bool):
    try:
        trigger_workflow_response = iwx_client.trigger_workflow(workflow_id=workflow_id, domain_id=domain_id)
        logger.debug("trigger_workflow() - Response: {}".format(json.dumps(trigger_workflow_response)))
        if trigger_workflow_response['result']['status'] == 'success':
            run_id = trigger_workflow_response['result']['response']['result']['id']
            logger.info(f"Triggered workflow {workflow_name_id_mappings.get(workflow_id, workflow_id)} successfully "
                        f"with run Id {run_id}")
        else:
            raise Exception("API status returned as Failed")

    except Exception as trigger_workflow_error:
        logger.exception(f"Failed to Trigger Workflow - Error: {trigger_workflow_error}")
        sys.exit(-100)

    if poll_workflow_bool:
        try:
            workflow_status_response = iwx_client.poll_workflow_run_till_completion(workflow_id=workflow_id,
                                                                                    workflow_run_id=run_id,
                                                                                    poll_interval=30)
            logger.debug("poll_workflow_run_till_completion() - "
                         "Response: {}".format(json.dumps(workflow_status_response)))

            workflow_status = workflow_status_response['result']['response'].get('result', {}) \
                .get('workflow_status', {}).get('state')

            logger.info(f"Workflow Run Execution Finished with status {workflow_status}")
        except Exception as error:
            logger.error(f"Failed to poll Workflow Status - Error: {error}")


def main():
    global iwx_client, logger, configuration_json, refresh_token, environment_id, metadata_csv_path, database_info_df, workflow_name_id_mappings,snowflake_metasync_source_id
    parser = argparse.ArgumentParser(description='Bulk Configuration')
    parser.add_argument('--source_id', type=str, required=True,
                        help='source_id for which tables needs to be configured')
    parser.add_argument('--refresh_token', type=str, required=True,
                        help='Pass the refresh token of user with admin privileges')
    parser.add_argument('--configure', type=str, default="all",
                        help='Pass the configure type (all(default),table,tg or workflow)',
                        choices=['all', 'table', 'tg', 'workflow'])
    parser.add_argument('--domain_name', type=str,
                        help='Pass the domain in which workflows are to be created.Creates new domain if not exists')
    parser.add_argument('--source_type', type=str, required=True, help='Enter source type teradata/oracle/netezza/snowflake/sql server',
                        choices=['teradata', 'oracle', 'vertica', 'netezza', 'mysql', 'snowflake', 'sqlserver'])
    parser.add_argument('--metadata_csv_path', type=str, required=True,
                        help='Pass the absolute path of metadata csv file')
    parser.add_argument('--config_json_path', type=str, required=True,
                        help='Pass the absolute path of configuration json file')
    parser.add_argument('--snowflake_metasync_source_id', type=str, required=False,
                        help='Pass the snowflake metasync source ID in infoworks')
    args = parser.parse_args()
    cwd = os.getcwd()
    try:
        # Configuring Logger
        log_file_path = f"{cwd}/logs"
        if not os.path.exists(log_file_path):
            os.makedirs(log_file_path)

        logs_folder = f'{cwd}/logs/'
        log_filename = 'tables_bulk_configuration.log'
        logger = configure_logger(logs_folder, log_filename)

        # Arguments Values
        source_id = args.source_id
        refresh_token = args.refresh_token
        configure_type = args.configure
        source_type = args.source_type
        metadata_csv_path = args.metadata_csv_path
        config_json_path = args.config_json_path
        domain_id = None
        snowflake_metasync_source_id = args.snowflake_metasync_source_id
        # Reading configurations
        configuration_file = open(config_json_path, "r")
        configuration_json = json.load(configuration_file)

        # Initiating Infoworks SDK Client
        infoworks.sdk.local_configurations.REQUEST_TIMEOUT_IN_SEC = 30
        infoworks.sdk.local_configurations.MAX_RETRIES = 3
        iwx_client = InfoworksClientSDK()
        iwx_client.initialize_client_with_defaults(configuration_json.get('protocol', 'http'),
                                                   configuration_json.get('host', 'localhost'),
                                                   configuration_json.get('port', '3001'),
                                                   args.refresh_token)
        # Fetching IWX Environment ID with Environment Name
        environment_id_response = iwx_client.get_environment_id_from_name(configuration_json["environment_name"])
        logger.debug(f"get_environment_id_from_name() - Response: {environment_id_response}")

        if environment_id_response['result']['status'] == "success":
            if environment_id_response['result']['response']['environment_id']:
                environment_id = environment_id_response['result']['response']['environment_id']
            else:
                raise Exception(f"Environment Id Not Found with Name {args.environment_name}")
        else:
            raise Exception(f"API Status Returned as Failed")

        if args.domain_name:
            domain_id = create_domain_if_not_exists(args.domain_name)
            # Adding source to domain
            domain_body = {"entity_ids": [source_id]}
            add_source_to_domain_response = iwx_client.add_source_to_domain(domain_id, config_body=domain_body)
            logger.debug(f"add_source_to_domain() - Response: {json.dumps(add_source_to_domain_response)}")
            if add_source_to_domain_response['result']['status'] == "success":
                logger.info("Added source to domain successfully")
            else:
                raise Exception("API status returned as Failed")

        if exists(metadata_csv_path):
            logger.info(f"Opening the {source_type} metadata CSV '{metadata_csv_path}' file for reading")
            database_info_df = pd.read_csv(metadata_csv_path)
            database_info_df = database_info_df.fillna('')
        else:
            logger.error(f"Did not find the file {metadata_csv_path}. Exiting.")
            exit(-1000)

        run_workflow_bool = configuration_json.get('run_workflow', True)
        poll_workflow_bool = configuration_json.get('poll_workflow', False)

        if run_workflow_bool not in [True, False] or poll_workflow_bool not in [True, False]:
            logger.error("Please provide either True / False to run_workflow and poll_workflow_bool"
                         " variables in configurations.json\n")
            exit(0)

        workflow_name_id_mappings = {}
        if configure_type == "all":
            tables_configure(source_id, True, source_type)
            workflow_list = configure_workflow(source_id, domain_id)
            logger.info(f"workflows list : {workflow_list}")
            for workflow_id in workflow_list:
                if workflow_id and run_workflow_bool:
                    run_workflow(workflow_id, domain_id, poll_workflow_bool)
        elif configure_type == 'table' or configure_type == 'tg':
            if configure_type == 'table':
                tables_configure(source_id, False, source_type)
            else:
                tables_configure(source_id, True, source_type)
        else:
            workflow_list = configure_workflow(source_id, domain_id)
            logger.info(f"workflows list : {workflow_list}")
            for workflow_id in workflow_list:
                if workflow_id and run_workflow_bool:
                    run_workflow(workflow_id, domain_id, poll_workflow_bool)

    except Exception as error:
        print(f"Failed to configure tables/workflows : {error}")

    finally:
        print(f"\n\n\nLogs for this script is available under {cwd}/logs/tables_bulk_configuration.log")


if __name__ == "__main__":
    global configuration_json, iwx_client, logger, refresh_token, environment_id, database_info_df, workflow_name_id_mappings,snowflake_metasync_source_id
    main()