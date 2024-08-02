import argparse
import json
import logging
import os
import sys
import traceback
from os.path import exists
import pandas as pd
from infoworks.sdk.client import InfoworksClientSDK
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from helper_utilities.utils import trim_spaces, print_error, print_success, print_warning, print_info


def initialise_logger():
    cwd = os.getcwd()
    log_file_path = f"{cwd}/logs"
    if not os.path.exists(log_file_path):
        os.makedirs(f"{cwd}/logs")
    formatter = logging.Formatter('%(asctime)s - %(module)s - %(pathname)s - %(lineno)d - %(levelname)s - %(message)s')
    handler = logging.FileHandler(f'{cwd}/logs/tables_bulk_configuration.log')
    handler.setFormatter(formatter)
    logger = logging.getLogger("tables_bulk_configuration_logs")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger


class TableBulkConfigure:
    def __init__(self, refresh_token, host, port, protocol):
        self.refresh_token = refresh_token
        self.iwx_client = InfoworksClientSDK()
        self.iwx_client.initialize_client_with_defaults(protocol, host, port, self.refresh_token)
        self.logger = initialise_logger()
        self.workflow_name_id_mappings = {}
        self.table_names_list = []
        self.database_info_df = None
        self.configuration_json = {}

    def configure_ingestion_strategy(self, table_id, columns, column_type_dict,
                                     table_payload_dict, iwx_column_name_mappings,
                                     column_name_case_compatible_dict):
        case_insensitive_merge_columns = [column_name_case_compatible_dict.get(i.upper(), '') for i in
                                          self.configuration_json["merge_water_marks_columns"]]
        case_insensitive_merge_columns = [i for i in case_insensitive_merge_columns if i != '']
        case_insensitive_append_columns = [column_name_case_compatible_dict.get(i.upper(), '') for i in
                                           self.configuration_json["append_water_marks_columns"]]
        case_insensitive_append_columns = [i for i in case_insensitive_append_columns if i != '']
        iwx_compatible_merge_Columns = [iwx_column_name_mappings[i] for i in case_insensitive_merge_columns if
                                        i in iwx_column_name_mappings.keys()]
        iwx_compatible_append_Columns = [iwx_column_name_mappings[i] for i in case_insensitive_append_columns if
                                         i in iwx_column_name_mappings.keys()]
        merge_Columns = [value for value in iwx_compatible_merge_Columns if value in columns]
        merge_Columns = [merge_col for merge_col in merge_Columns if column_type_dict[merge_col] in [91, 93, 4]]
        append_Columns = [value for value in iwx_compatible_append_Columns if value in columns]
        append_Columns = [append_col for append_col in append_Columns if column_type_dict[append_col] in [91, 93, 4]]
        try:
            ingestion_strategy = \
                self.database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')['INGESTION_STRATEGY'].tolist()[
                    0].strip()
        except (IndexError, KeyError) as e:
            print_warning("Did not find the ingestion_strategy in csv file. Defaulting to FULL_REFRESH")
            self.logger.warning("Did not find the ingestion_strategy in csv file. Defaulting to FULL_REFRESH")
            ingestion_strategy = 'FULL_REFRESH'
        print_info("ingestion_strategy: " + ingestion_strategy)
        self.logger.info(f"ingestion_strategy: {ingestion_strategy}")
        if ingestion_strategy not in ['INCREMENTAL_APPEND', 'INCREMENTAL_MERGE', 'FULL_REFRESH', '']:
            print_error(
                "Provided invalid option for INGESTION_STRATEGY.\nShould be one of the following options:\n["
                "'INCREMENTAL_APPEND','INCREMENTAL_MERGE','FULL_REFRESH']")
            self.logger.error(
                "Provided invalid option for INGESTION_STRATEGY.\nShould be one of the following options:\n["
                "'INCREMENTAL_APPEND','INCREMENTAL_MERGE','FULL_REFRESH']")
        else:
            if ingestion_strategy == 'INCREMENTAL_MERGE' and len(table_payload_dict["natural_keys"]) > 0:
                table_payload_dict["sync_type"] = "incremental"
                table_payload_dict["update_strategy"] = "merge"
                is_scd2_type = False
                try:
                    scd_type2_val = str(
                        self.database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')['SCD_TYPE_2'].tolist()[
                            0]).strip()
                    if eval(scd_type2_val.capitalize()) in [False, True]:
                        is_scd2_type = eval(scd_type2_val.capitalize())

                except (KeyError, IndexError) as e:
                    print_warning(f"Did not find the SCD_TYPE_2 column in csv defaulting to SCD TYPE 1 for merge")
                    self.logger.warning(f"Did not find the SCD_TYPE_2 column in csv defaulting to SCD TYPE 1 for merge")

                if not is_scd2_type:
                    table_payload_dict["is_scd2_table"] = False
                else:
                    # TO-DO Add SCD2 specific entries
                    table_payload_dict["is_scd2_table"] = True
                merge_column = []
                try:
                    merge_column = \
                        self.database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')[
                            'WATERMARK_COLUMN'].tolist()
                    if merge_column != []:
                        merge_column = list(map(trim_spaces, merge_column[0].split(',')))
                    merge_column = [column_name_case_compatible_dict[i.upper()] for i in merge_column]
                except IndexError as e:
                    print_error(
                        f"Did not find the watermark columns in CSV even though the strategy is INCREMENTAL_MERGE. "
                        f"Defaulting to first merge Column found: {merge_Columns[0]}")
                    self.logger.error(
                        f"Did not find the watermark columns in CSV even though the strategy is INCREMENTAL_MERGE. "
                        f"Defaulting to first merge Column found: {merge_Columns[0]}")
                    if len(merge_column) != 0:
                        merge_column = merge_Columns[0]
                    else:
                        print_warning("Defaulting to Full Refresh as watermark_column is not available in csv")
                        self.logger.info("Defaulting to Full Refresh as watermark_column is not available in csv")
                        table_payload_dict["sync_type"] = "full-load"
                try:
                    table_payload_dict["watermark_columns"] = [iwx_column_name_mappings[i] if \
                        iwx_column_name_mappings[i] in columns else merge_Columns[0] for i in merge_column]
                except IndexError as e:
                    print_error(
                        f"Columns in IWX tables are {columns}, but found non existing column {merge_column} for merge")
                    self.logger.error(
                        f"Columns in IWX tables are {columns}, but found non existing column {merge_column} for merge")
                table_payload_dict["is_scd2_table"] = is_scd2_type
                watermark_column = \
                    [iwx_column_name_mappings[i] if \
                         iwx_column_name_mappings[i] in columns else merge_Columns[0] for i in merge_column]
                print_info("Configuring table for incremental merge mode")
                self.logger.info("Configuring table for incremental merge mode")
                print_info(f"Setting Merge Mode with watermark Column : {watermark_column}")
                self.logger.info(f"Setting Merge Mode with watermark Column : {watermark_column}")
            elif ingestion_strategy == 'INCREMENTAL_APPEND' and len(table_payload_dict["natural_keys"]) > 0:
                table_payload_dict["sync_type"] = "incremental"
                table_payload_dict["update_strategy"] = "append"
                try:
                    append_column = \
                        self.database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')[
                            'WATERMARK_COLUMN'].tolist()[0].strip()
                    if append_column != []:
                        append_column = list(map(trim_spaces, append_column[0].split(',')))
                    append_column = column_name_case_compatible_dict[append_column.upper()]
                    append_column = [append_col for append_col in append_column if
                                     column_type_dict[append_col] in [91, 93, 4]]
                except IndexError as e:
                    print_error(
                        f"Did not find the watermark columns in CSV even though the strategy is INCREMENTAL_APPEND. "
                        f"Defaulting to first append Column found: {append_Columns[0]}")
                    self.logger.error(
                        f"Did not find the watermark columns in CSV even though the strategy is INCREMENTAL_APPEND. "
                        f"Defaulting to first append Column found: {append_Columns[0]}")
                    append_column = append_Columns[0]
                table_payload_dict["watermark_columns"] = [iwx_column_name_mappings[i] if \
                        iwx_column_name_mappings[i] in columns else append_Columns[0] for i in append_column]
                watermark_column = [iwx_column_name_mappings[i] if \
                        iwx_column_name_mappings[i] in columns else append_Columns[0] for i in append_column]
                print_info("Configuring table for incremental append mode")
                self.logger.info("Configuring table for incremental append mode")
                print_info(f"Setting Append Mode with watermark Column : {watermark_column}")
                self.logger.info(f"Setting Append Mode with watermark Column : {watermark_column}")
            else:
                print_info("Defaulting to Full Refresh as there is no natural key or incremental strategy")
                self.logger.info("Defaulting to Full Refresh")
                table_payload_dict["sync_type"] = "full-load"
                pass
        print(table_payload_dict)
        return table_payload_dict

    def configure_partition_for_table(self, table_id, columns, column_type_dict, table_payload_dict,
                                      iwx_column_name_mappings,
                                      column_name_case_compatible_dict):
        try:
            try:
                partition_column_details = \
                    self.database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')['PARTITION_COLUMN'].tolist()[
                        0].strip()
                partition_column_details = partition_column_details.replace("TRUE", "true").replace("True", "true")
                partition_column_details = partition_column_details.replace("FALSE", "false").replace("False", "false")
                partition_column_details = json.loads(partition_column_details)

                partition_column = column_name_case_compatible_dict[
                    partition_column_details.get("PARTITION_COLUMN","").upper()] if partition_column_details.get("PARTITION_COLUMN","")!="" else ""
                partition_column = iwx_column_name_mappings[partition_column] if partition_column_details.get("PARTITION_COLUMN","")!="" else ""
            except (KeyError, IndexError, json.decoder.JSONDecodeError) as e:
                print_error(str(e) + " In configure_partition_for_table codebase")
                self.logger.error(str(e) + " In configure_partition_for_table codebase")
                return table_payload_dict
            if partition_column in columns:
                print_info(f"Adding partition column {partition_column}..")
                self.logger.info(f"Adding partition column {partition_column}..")
                if column_type_dict[partition_column] not in [91, 93]:
                    table_payload_dict["partition_keys"] = [
                        {
                            "partition_column": partition_column,
                            "is_derived_partition": False
                        }]
                else:
                    derived_partition = partition_column_details.get("DERIVED_PARTITION", False)
                    if derived_partition:
                        derived_format = partition_column_details.get("DERIVED_FORMAT", "yyyy")
                        allowed_values_for_date_partition = ['dd', 'MM', 'yyyy', 'yyyyMM', 'MMdd', 'yyyyMMdd']
                        derive_func_map = {'dd': "day num in month", 'MM': "month", 'yyyy': "year",
                                           'yyyyMM': "year month", 'MMdd': "month day",
                                           'yyyyMMdd': "year month day"}
                        if derived_format not in allowed_values_for_date_partition:
                            print_error(
                                f"derived_format column in CSV should be one of {','.join(allowed_values_for_date_partition)}. Unknown property {derived_format} provided")
                            self.logger.error(
                                f"derived_format column in CSV should be one of {','.join(allowed_values_for_date_partition)}. Unknown property {derived_format} provided")

                        else:
                            print_info(
                                f"Deriving the {derive_func_map[derived_format]} from {partition_column} for partition")
                            self.logger.info(
                                f"Deriving the {derive_func_map[derived_format]} from {partition_column} for partition")
                            derived_partition_column_name = partition_column_details.get("DERIVED_PARTITION_COL_NAME",
                                                                                         "iw_partition_column_" + partition_column.lower())
                            table_payload_dict["partition_keys"] = [
                                {
                                    "parent_column": partition_column,
                                    "derive_format": derived_format,
                                    "derive_function": derive_func_map[derived_format],
                                    "is_derived_partition": True,
                                    "partition_column": derived_partition_column_name
                                }]
                    else:
                        table_payload_dict["partition_keys"] = [
                            {
                                "partition_column": partition_column,
                                "is_derived_partition": False
                            }]
                print_success(f"Added partition column {partition_column} successfully!")
                self.logger.info(f"Added partition column {partition_column} successfully!")
            else:
                if partition_column != '':
                    print_error(
                        f"{partition_column} column not found in this table. Columns available in table: {','.join(columns)}\n Skipping partition...")
                    self.logger.error(
                        f"{partition_column} column not found in this table. Columns available in table: {','.join(columns)}\n Skipping partition...")
                else:
                    print_info("Skipping partition for table.")
                    self.logger.info("Skipping partition for table.")
        except Exception as e:
            traceback.print_exc()
            print_error(f"Did not find the partition column in csv file. Skipping partition\n{str(e)}")
            self.logger.error(f"Did not find the partition column in csv file. Skipping partition\n{str(e)}")

        return table_payload_dict

    def configure_table_group(self, table_group_object, source_id):
        print_info("\n\nConfiguring table group with below configurations")
        self.logger.info("Configuring table group with below configurations")
        print_info(str(table_group_object))
        self.logger.info(table_group_object)
        client_response = self.iwx_client.get_list_of_table_groups(source_id,
                                                                   params={
                                                                       "filter": {"name": table_group_object['name']}})
        tg_id = client_response.get("result")["response"]["result"]
        try:
            if len(tg_id) > 0:
                print_success(
                    f"Found an existing table group in the source with the same name {table_group_object['name']}, Updating the found table group\n")
                self.logger.info(
                    f"Found an existing table group in the source with the same name {table_group_object['name']}, Updating the found table group\n")
                response = self.iwx_client.update_table_group(source_id, tg_id[0]["id"], table_group_object)
                if response["result"]["status"] != "success":
                    print_error(f"Failed to configure table group {table_group_object['name']}")
                    self.logger.error(f"Failed to configure table group {table_group_object['name']}")
                    self.logger.error(response["error"])
                else:
                    print_success(f"Configured table group {table_group_object['name']} successfully!")
                    self.logger.info(f"Configured table group {table_group_object['name']} successfully!")
            else:
                response = self.iwx_client.create_table_group(source_id, table_group_object)
                if response["result"]["status"] != "success":
                    print_error(f"Failed to create table group {table_group_object['name']}")
                    self.logger.error(f"Failed to create table group {table_group_object['name']}")
                    self.logger.error(response["error"])

                else:
                    print_success(f"Created table group {table_group_object['name']} successfully!")
                    self.logger.info(f"Created table group {table_group_object['name']} successfully!")
        except Exception as e:
            print_error(str(e))
            self.logger.error(str(e))

    def tables_configure(self, source_id, configure_table_group_bool):
        list_tables_response = self.iwx_client.list_tables_in_source(source_id)
        tables = list_tables_response.get("result", {}).get("response",{}).get("result",[])
        if tables == []:
            print_error(f"Invalid source ID {source_id}.Please validate the same. Exiting...")
            exit(-100)
        if len(tables) == 0:
            print_error(f"No tables found under the source {source_id}")
            self.logger.error(f"No tables found under the source {source_id}")
            exit(-1000)
        tables_details = {}
        for table in tables:
            try:
                if table["name"] not in self.table_names_list:
                    continue
                table_id = str(table['id'])
                table_name = table["name"]
                tables_details[table_name] = table_id
                table_payload_dict = {}
                table_id = str(table['id'])
                table_name = table['original_table_name']
                columns = []
                column_type_dict = {}
                col_object_array = table.get('columns', [])
                if not col_object_array:
                    continue
                iwx_column_name_mappings = {}
                column_name_case_compatible_dict = {}
                for col_object in table.get('columns', []):
                    columns.append(col_object['name'])
                    column_type_dict[col_object['name']] = col_object['target_sql_type']
                    iwx_column_name_mappings[col_object['name']] = col_object['name']
                    column_name_case_compatible_dict[col_object['name'].upper()] = col_object['name']
                print_info("TableName : " + table['original_table_name'])
                self.logger.info(f"TableName : {table['original_table_name']}")
                print_info("Columns : " + str(columns))
                self.logger.info(f"Columns : {columns}")
                storage_format = self.configuration_json.get("ingestion_storage_format", "delta").strip().lower()
                try:
                    storage_format_user = \
                        self.database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna(storage_format)[
                            'STORAGE_FORMAT'].tolist()[0].strip().lower()
                    # print(storage_format_user)
                except IndexError as e:
                    print_warning(
                        f"Defaulting to storage format {storage_format} as STORAGE_FORMAT column was not found in csv")
                    self.logger.warning(
                        f"Defaulting to storage format {storage_format} as STORAGE_FORMAT column was not found in csv")
                    storage_format_user = storage_format

                if storage_format_user in ["orc", "parquet", "avro", "delta"]:
                    storage_format = storage_format_user
                else:
                    print_error("Please provide the valid storage format(orc,parquet,delta or avro)\nSkipping..")
                    self.logger.error("Please provide the valid storage format(orc,parquet,delta or avro)\nSkipping..")
                    raise

                print_info("storage_format: " + storage_format)
                # configure natural keys
                probable_natural_keys = []
                try:
                    probable_natural_keys = self.database_info_df.query(f"IWX_TABLE_ID == '{table_id}'")[
                        'PROBABLE_NATURAL_KEYS'].tolist()
                    probable_natural_keys = [x.strip('') for x in probable_natural_keys if type(x) == str]
                except (IndexError, KeyError) as e:
                    print_warning("No natural keys found. Skipping configuration of natural keys")
                    self.logger.warning("No natural keys found. Skipping configuration of natural keys")
                    pass

                if probable_natural_keys != []:
                    probable_natural_keys = list(map(trim_spaces, probable_natural_keys[0].split(',')))

                    try:
                        probable_natural_keys = [column_name_case_compatible_dict[i.upper()] for i in
                                                 probable_natural_keys]
                        probable_natural_keys = [iwx_column_name_mappings[i] for i in probable_natural_keys]
                    except KeyError as e:
                        print_error(
                            f"Columns present in this table are : {columns}\n Unknown column provided {str(e)}\n Please validate the same and rerun the script")
                        self.logger.error(
                            f"Columns present in this table are : {columns}\n Unknown column provided {str(e)}\n Please validate the same and rerun the script")
                else:
                    probable_natural_keys = []
                if len(probable_natural_keys) != 0:
                    print_info("probable_natural_keys found are " + str(probable_natural_keys))
                    self.logger.info(f"probable_natural_keys found are : {probable_natural_keys}")
                    table_payload_dict["natural_keys"] = probable_natural_keys
                else:
                    table_payload_dict["natural_keys"] = []

                self.logger.info(f"probable_natural_keys found are : {probable_natural_keys}")
                table_payload_dict = self.configure_ingestion_strategy(table_id, columns, column_type_dict,
                                                                       table_payload_dict, iwx_column_name_mappings,
                                                                       column_name_case_compatible_dict)

                print_info(str(table_payload_dict))

                # configure table partition
                table_payload_dict = self.configure_partition_for_table(table_id, columns, column_type_dict,
                                                                        table_payload_dict,
                                                                        iwx_column_name_mappings,
                                                                        column_name_case_compatible_dict)
                table_payload_dict["storage_format"] = storage_format
                # configure target table schema and target table name
                target_table_name, target_schema_name, target_database_name = '', '', ''
                try:
                    target_table_name = \
                        self.database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna('')[
                            'TARGET_TABLE_NAME'].tolist()[
                            0].strip()
                    target_schema_name = \
                        self.database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna('')[
                            'TARGET_SCHEMA_NAME'].tolist()[
                            0].strip()
                    target_database_name = \
                        self.database_info_df.query(f"IWX_TABLE_ID == '{table_id}'").fillna('')[
                            'TARGET_DATABASE_NAME'].tolist()[
                            0].strip()
                except (IndexError, KeyError) as e:
                    print_warning(
                        "Defaulting to original table name and schema name as target table name and schema name as the "
                        "entry was not found in csv")
                    self.logger.info(
                        "Defaulting to original table name and schema name as target table name and schema name as the "
                        "entry was not found in csv")
                    pass

                if target_table_name != '':
                    print_info("target_table_name : " + target_table_name)
                    self.logger.info(f"target_table_name : {target_table_name}")
                    table_payload_dict["target_table_name"] = target_table_name
                if target_schema_name != '':
                    print_info("target_schema_name : " + target_schema_name)
                    self.logger.info(f"target_schema_name : {target_schema_name}")
                    table_payload_dict["target_schema_name"] = target_schema_name

                if target_database_name != '':
                    print_info("target_database_name : " + target_database_name)
                    self.logger.info(f"target_database_name : {target_database_name}")
                    table_payload_dict["target_database_name"] = target_database_name

                # is user managed table
                user_managed_table = True
                try:
                    user_managed_table = str(
                        self.database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')[
                            'USER_MANAGED_TABLE'].tolist()[
                            0]).strip()
                    if eval(user_managed_table.capitalize()) in [False, True]:
                        user_managed_table = eval(user_managed_table.capitalize())
                except (KeyError, IndexError) as e:
                    print_error(f"Did not find the USER_MANAGED_TABLE column in csv defaulting to True for merge")
                    self.logger.error(f"Did not find the USER_MANAGED_TABLE column in csv defaulting to True for merge")
                if user_managed_table:
                    table_payload_dict["is_table_user_managed"] = True
                    table_payload_dict["does_table_contain_data"] = True
                else:
                    table_payload_dict["is_table_user_managed"] = False

                max_modified_timestamp=""
                try:
                    max_modified_timestamp = str(
                        self.database_info_df.query(f"IWX_TABLE_ID=='{table_id}'").fillna('')[
                            'MAX_MODIFIED_TIMESTAMP'].tolist()[
                            0]).strip()
                except (KeyError, IndexError) as e:
                    print_error(f"Did not find max modified timestamp column value in csv. Will skip setting the value for it.")
                    self.logger.error(f"Did not find max modified timestamp column value in csv. Will skip setting the value for it.")
                print("table_payload_dict:",table_payload_dict)
                client_response = self.iwx_client.configure_table_ingestion_properties_with_payload(source_id, table_id,
                                                                                                    table_payload_dict)
                print_info("Table configuration API response "+ json.dumps(client_response))
                self.logger.info("Table configuration API response " + json.dumps(client_response))
                if max_modified_timestamp:
                    table_temp_payload={"name":table_name,"source":source_id,"max_modified_timestamp":max_modified_timestamp}
                    resp = self.iwx_client.update_table_configuration(source_id,table_id,table_temp_payload)

            except Exception as e:
                print_error(f"Failed to configure {table}: " + str(e))

        if configure_table_group_bool:
            tg_grp = self.database_info_df.groupby("TABLE_GROUP_NAME").agg(list)
            for index, row in tg_grp.iterrows():
                table_ids = list(set(row["IWX_TABLE_ID"]))
                table_group_name = index
                tables = [{"table_id": i} for i in table_ids if not pd.isna(i)]

                try:
                    ENVIRONMENT_COMPUTE_NAME = \
                        [i for i in list(set(row["ENVIRONMENT_COMPUTE_NAME"])) if i != '' and i is not None][0]
                except KeyError:
                    ENVIRONMENT_COMPUTE_NAME = self.configuration_json["environment_compute_name"]
                try:
                    WAREHOUSE_NAME = [i for i in list(set(row["WAREHOUSE_NAME"])) if i != '' and i is not None][0]
                except KeyError:
                    environment_name = self.configuration_json["environment_name"]
                    get_environment_id_from_name_response = self.iwx_client.get_environment_id_from_name(
                        environment_name)
                    environment_id = get_environment_id_from_name_response.get("result", {}).get("response", {}).get(
                        "environment_id", None)
                    get_environment_default_warehouse_response = self.iwx_client.get_environment_default_warehouse(
                        environment_id)
                    WAREHOUSE_NAME = get_environment_default_warehouse_response.get("result", {}).get("response", {}).get("default_warehouse")
                environment_compute_template = {
                    "environment_compute_template_id": self.iwx_client.get_compute_id_from_name(
                        self.iwx_client.get_environment_id_from_name(self.configuration_json["environment_name"]),
                        ENVIRONMENT_COMPUTE_NAME)}

                if len(tables) > 0:
                    table_group_object = {
                        "source": source_id,
                        "environment_compute_template": environment_compute_template,
                        "name": table_group_name,
                        "max_parallel_entities": len(tables),
                        "warehouse": WAREHOUSE_NAME,
                        "tables": tables
                    }
                    self.configure_table_group(table_group_object, source_id)


def main():
    parser = argparse.ArgumentParser(description='CSV/Fixed Width Source Bulk table Configuration')
    parser.add_argument('--configure', type=str, default="all",
                        help='Pass the configure type (all(default),table,tg)')
    parser.add_argument('--metadata_csv_path', type=str, required=True,
                        help='Pass the absolute path of metadata csv file')
    parser.add_argument('--config_json_path', type=str, required=True,
                        help='Pass the absolute path of configuration json file')
    args = parser.parse_args()
    configure_type = args.configure
    metadata_csv_path = args.metadata_csv_path
    config_json_path = args.config_json_path
    configuration_file = open(config_json_path, "r")
    configuration_json = json.load(configuration_file)
    host = configuration_json.get('host', 'localhost')
    port = configuration_json.get('port', '3001')
    protocol = configuration_json.get('protocol', 'http')
    refresh_token = configuration_json.get("refresh_token", "")
    if refresh_token == "":
        print("Please pass the refresh_token variable in configuration.json file.Exiting..")
        exit(-100)
    table_configure_obj = TableBulkConfigure(refresh_token, host, port, protocol)
    cwd = os.getcwd()
    if exists(metadata_csv_path):
        database_info_df = pd.read_csv(metadata_csv_path)
    else:
        print_error(f"Did not find the file {metadata_csv_path} for reading")
        table_configure_obj.logger.error(f"Did not find the file {metadata_csv_path} for reading")
        print("Exiting...")
        exit(-1000)
    source_id = database_info_df["IWX_SOURCE_ID"].iloc[0]
    tables_list = database_info_df['TABLE_NAME'].to_list()
    table_names_list = [i.split(".")[-1] for i in tables_list]
    # configuration_file= open(f"{cwd}/conf/configurations.json","r")
    if configure_type not in ['all', 'table', 'tg']:
        print_error(
            f"Unknown configure type {configure_type} \nPlease provide the valid configure type (all,table,tg)")
        table_configure_obj.logger.error(
            f"Unknown configure type {configure_type} \nPlease provide the valid configure type (all,table,tg)")
        exit(0)
    table_configure_obj.database_info_df = database_info_df
    table_configure_obj.table_names_list = table_names_list
    table_configure_obj.configuration_json = configuration_json
    try:
        if configure_type == 'all':
            table_configure_obj.tables_configure(source_id, True)
        else:
            if configure_type == 'table':
                table_configure_obj.tables_configure(source_id, False)
            else:
                table_configure_obj.tables_configure(source_id, True)
    finally:
        configuration_file.close()
        print(f"\n\n\nLogs for this script is available under {cwd}/logs/csv_tables_bulk_configuration.log")


if __name__ == '__main__':
    main()
