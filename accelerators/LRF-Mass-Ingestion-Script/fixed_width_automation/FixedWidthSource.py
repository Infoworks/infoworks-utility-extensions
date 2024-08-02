import argparse
import logging
import os
import sys
import traceback
import re
import numpy as np
from infoworks.sdk.client import InfoworksClientSDK
import json
import csv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from helper_utilities.utils import print_success, print_error, print_info, validate_lrf_layout, is_int, is_float, \
    set_src_advanced_config, \
    create_src_extensions, dump_table_details, print_warning
from FileBasedAbstractClass import FileBasedIWXSource
from local_configurations import SCHEMA_LOOKUP as schema_lookup, \
    FIXED_WIDTH_SOURCE_EXTENSION_NAME as SOURCE_EXTENSION_NAME, \
    UDF_INPUT_SQL_TYPE_MAPPING, column_val_datatype_mapping, MAX_WORKERS, nvl2_datatype_mapping


# Initialise logger
def initialise_logger():
    cwd = os.getcwd()
    log_path = "/".join(cwd.split("/")[:-1])
    log_file_path = f"{log_path}/logs"
    if not os.path.exists(log_file_path):
        os.makedirs(f"{log_file_path}")
    formatter = logging.Formatter('%(asctime)s - %(module)s - %(pathname)s - %(lineno)d - %(levelname)s - %(message)s')
    handler = logging.FileHandler(f'{log_file_path}/fixed_width_tables_mass_ingestion.log')
    handler.setFormatter(formatter)
    logger = logging.getLogger("fixed_width_mass_ingestion_logs")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger


class FixedWidthSource(FileBasedIWXSource):
    def __init__(self, refresh_token, host, port, protocol):
        self.refresh_token = refresh_token
        self.iwx_client = InfoworksClientSDK()
        self.iwx_client.initialize_client_with_defaults(protocol, host, port, self.refresh_token)
        self.logger = initialise_logger()
        with open("./conf/configurations.json") as configuration_json_file:
            self.configurations_json = json.load(configuration_json_file)
        with open("../conf/teradata_spark_dateformat_mappings.json") as teradata_date_map_file:
            self.teradata_spark_mappings = json.load(teradata_date_map_file)
        self.extension_mapping = {}
        with open("conf/extension_mapping.csv", "r") as file:
            csv_file = csv.DictReader(file)
            for row in csv_file:
                self.extension_mapping[row["parsed_extension"]] = {"iwx_extension_name": row["iwx_extension_name"],
                                                                   "search_type": row["search_type"],
                                                                   "params": row["params"]}
        self.default_file_properties = {
            "encoding": "UTF-8",
            "escape_character": "\\",
            "header_rows_count": 0,
            "record_width_type": "fixed_width",
            "record_separator": "\\n",
            "columns_configurations": []
        }
        self.table_schema_df = None
        self.snowflake_default_props = {}
        self.skipped_tables = []

    def set_table_df(self, table_schema_path):
        try:
            self.table_schema_df = pd.read_csv(table_schema_path).replace(np.nan, None, regex=True)
        except TypeError:
            self.table_schema_df = pd.read_csv(table_schema_path)

        self.table_schema_df.drop_duplicates(subset="TABLE_NAME", keep="last", inplace=True)
        print_success(f"{self.table_schema_df.shape[0]} UNIQUE TABLES FOUND IN THE INPUT CSV")

    # overriding abstract method
    def create_source(self, source_creation_body):
        source_name = source_creation_body['name']
        get_sourceid_from_name_response = self.iwx_client.get_list_of_sources(params={"filter":{"name":source_name}})
        source_id_list = get_sourceid_from_name_response.get("result",{}).get("response",{}).get("result",[])
        source_id = None
        if len(source_id_list)>0:
            source_id=source_id_list[0]["id"]

        print_info(f"Creating FixedWidth Source {source_name}")
        self.logger.info(f"Creating FixedWidth Source {source_name}")
        if source_id:
            print_success(f"Found existing source with same name. Using the source id of the same {source_id}")
            self.logger.info(f"Found existing source with same name. Using the source id of the same {source_id}")
            return source_id
        else:
            client_response = self.iwx_client.create_source(source_config=source_creation_body)
            if client_response["result"]["status"] == "success":
                print_success(f"Created FixedWidth Source {source_name} with {client_response['result']['response']['result']['id']}")
                self.logger.info(
                    f"Created FixedWidth Source {source_name} with {client_response['result']['response']['result']['id']}")
                return client_response['result']['response']['result']['id']
            else:
                print_error(f"Failed to Create FixedWidth Source {source_name}")
                print_error(f"{client_response}")
                self.logger.error(f"Failed to Create FixedWidth Source {source_name}")
                self.logger.error(f"{client_response}")

    def configure_source_connection(self, source_id, source_connection_details_body):
        client_response = self.iwx_client.configure_source_connection(source_id, source_connection_details_body)
        if client_response["result"]["status"] == "success":
            print_success(f"Configured FixedWidth Source Connection for {source_id}")
            self.logger.info(f"Configured FixedWidth Source Connection for {source_id}")
        else:
            print_error(f"Failed to Configure FixedWidth Source Connection for {source_id}")
            self.logger.error(f"Failed to Configure FixedWidth Source Connection for {source_id}")
        return client_response

    def configure_file_mappings(self, source_id, file_mappings_body_serialised, reconfigure_tables):
        file_mappings_body = json.loads(file_mappings_body_serialised)
        client_response = self.iwx_client.add_table_and_file_mappings_for_csv(source_id, file_mappings_body)
        if client_response["result"]["status"] == "success":
            table_id = client_response["result"]["response"]["result"]["id"]
            print_success(f'Created FileMappings for {file_mappings_body["name"]}')
            self.logger.info(f'Created FileMappings for {file_mappings_body["name"]}')
        else:
            table_name = file_mappings_body["name"]
            print_warning(f"Table with {table_name} already exists in the source. Trying to reconfigure")
            self.logger.warning(f"Table with {table_name} already exists in the source. Trying to reconfigure")
            table_id = self.iwx_client.get_tableid_from_name(source_id, table_name)
            if reconfigure_tables:
                print_info("Trying to update table configurations if possible")
                self.logger.info("Trying to update table configurations if possible")
                self.iwx_client.update_table_configuration(source_id, table_id, file_mappings_body)
        print_success(f"Configured/Reconfigured file mappings for {file_mappings_body['name']}")
        self.logger.info(f"Configured/Reconfigured file mappings for {file_mappings_body['name']}")
        return table_id

    def configure_tables(self, source_id, table_id, table_name, table_config, snowflake_source_id):
        print_info(f"Configuring table {table_name}")
        self.logger.info(f"Configuring table {table_name}")
        try:
            table_schema = table_config["target_schema_name"]
            table_database = table_config["target_database_name"]
            table_name = table_name.upper()
            sf_table_name = table_config["target_table_name"]
            snowflake_columns_details = {}
            if snowflake_source_id is not None:
                print("snowflake_get_Call_details")
                print(f"{snowflake_source_id},{sf_table_name},{table_schema},{table_database}")
                try:
                    client_response = self.iwx_client.get_table_columns_details(snowflake_source_id,
                                                                            sf_table_name,
                                                                            table_schema, table_database)
                    if client_response["result"]["status"] == "success":
                        snowflake_columns_details = client_response["result"]["response"]
                    else:
                        print_error(f"Unable to fetch column details from source {snowflake_source_id}")
                        self.logger.error(f"Unable to fetch column details from source {snowflake_source_id}")
                        self.skipped_tables.append(
                            (table_name, f"Unable to fetch column details from source {snowflake_source_id}.",None, None))
                except Exception as e:
                    print("Failed while getting snowflake look up details")
                    self.skipped_tables.append(
                        (table_name, f"Unable to fetch column details from source {snowflake_source_id}.",None, None))
                    print(str(e))

            if snowflake_columns_details == {}:
                return None
            snowflake_columns_list = [i.upper() for i in snowflake_columns_details.keys()]
            columns_to_add = []
            lrf_column_names = self.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')[
                'LRF_SCHEMA'].to_list()[0].split(',')
            column_mappings = self.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')[
                'TABLE_SCHEMA_MAPPINGS'].to_dict()
            column_mappings = json.loads(list(column_mappings.values())[0])
            LRF_to_SF_col_mapping = defaultdict(lambda: [])
            for k, v in column_mappings.items():
                LRF_to_SF_col_mapping[v].append(k)
            # LRF_to_SF_col_mapping = dict((v, k) for k, v in column_mappings.items())
            function_mappings = self.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')[
                'FUNCTION_MAPPINGS'].to_dict()
            function_mappings = list(function_mappings.values())[0]
            function_mappings = json.loads(function_mappings)
            function_mappings = dict((k, v.strip()) for k, v in function_mappings.items())
            self.logger.debug(f"LRF COLUMN NAMES for {table_name} are " + " , ".join(lrf_column_names))
            # Create each column with proper datatypes and function mappings
            for lrf_column in lrf_column_names:
                column_name_in_lrf = lrf_column
                try:
                    column_name_in_targetdb = LRF_to_SF_col_mapping.get(lrf_column, []).pop(0)
                except IndexError:
                    column_name_in_targetdb = lrf_column
                temp = {"column_type": "source", "sql_type": 12, "target_sql_type": 12, "is_deleted": False,
                        "name": column_name_in_targetdb, "original_name": column_name_in_lrf, "is_audit_column": False}
                if column_name_in_targetdb not in snowflake_columns_list:
                    temp["is_excluded_column"] = True

                # Do a column lookup to snowflake
                col_snowflake_lookup_details = snowflake_columns_details.get(column_name_in_targetdb, {})
                if col_snowflake_lookup_details is not {}:
                    if col_snowflake_lookup_details.get("target_sql_type", ""):
                        temp["target_sql_type"] = col_snowflake_lookup_details.get("target_sql_type")
                    if col_snowflake_lookup_details.get("target_precision", ""):
                        temp["target_precision"] = col_snowflake_lookup_details["target_precision"]
                    if col_snowflake_lookup_details.get("target_scale", ""):
                        temp["target_scale"] = col_snowflake_lookup_details["target_scale"]
                    if col_snowflake_lookup_details.get("col_size", ""):
                        temp["col_size"] = col_snowflake_lookup_details["col_size"]

                # To handle cases where formats are not defined in the mload ctl files
                if temp["target_sql_type"] == 91:
                    temp["format"] = "yyyy-MM-dd"
                elif temp["target_sql_type"] == 93:
                    temp["format"] = "yyyy-MM-dd HH:mm:ss"

                # Apply corresponding function mappings for each column
                if function_mappings.get(column_name_in_targetdb, None) is not None:
                    mapping = function_mappings.get(column_name_in_targetdb)
                    if mapping.startswith(":") and (
                            "TO_INT" in mapping.upper() or "TO_DATE" in mapping.upper() or "TO_TIMESTAMP" in mapping.upper()):
                        pat = "(:.*)?\s*(TO_INT.*|TO_DATE.*|TO_TIMESTAMP.*)"
                        temp_list = re.findall(pat, mapping.upper())
                        if temp_list[0][0].strip().startswith(":"):
                            mapping = temp_list[0][-1]
                    if mapping.lower().strip().startswith("to_date"):
                        if temp["target_sql_type"] == 91:
                            format_val = mapping.replace("to_date(", "").replace(")", "").strip().strip("'").lower()
                            temp["format"] = self.teradata_spark_mappings.get(format_val, "yyyy-MM-dd")
                    elif mapping.lower().strip().startswith("to_timestamp"):
                        if temp["target_sql_type"] == 93:
                            format_val = "".join(mapping.replace("to_timestamp(", "").rsplit(")", 1)).strip().strip(
                                "'").lower()
                            temp["format"] = self.teradata_spark_mappings.get(format_val, "yyyy-MM-dd HH:mm:ss")
                    elif mapping.lower().strip().startswith("nvl2"):
                        nvl2_contents = mapping.replace("nvl2(", "").replace(",NULL)", "")
                        if nvl2_contents.startswith("to_date") or nvl2_contents.startswith("to_timestamp"):
                            try:
                                format_val = re.search("(?:to_date|to_timestamp)\s*\(.*\s*,\s*'(.*)'\s*\)", nvl2_contents).group(1)
                            except Exception as e:
                                format_val = ""
                            second_val = "yyyy-MM-dd" if nvl2_contents.startswith("to_date") else "yyyy-MM-dd HH:mm:ss"
                            temp["format"] = self.teradata_spark_mappings.get(format_val.lower(), second_val)
                            nvl2_contents = nvl2_contents.replace(format_val,self.teradata_spark_mappings.get(format_val.lower(), second_val))
                        temp["transformation_extension_name"] = SOURCE_EXTENSION_NAME
                        temp["transformation_function_alias"] = "nvl2"
                        temp["params_details"] = [{"value": nvl2_contents},
                                                  {"value": nvl2_datatype_mapping.get(temp["target_sql_type"])}]
                    else:
                        try:
                            transformation_function_alias, params_string = list(
                                re.match("(.*)\((.*)\)", mapping).groups())
                            temp["transformation_extension_name"] = SOURCE_EXTENSION_NAME
                            temp["transformation_function_alias"] = transformation_function_alias
                            temp["params_details"] = []
                            if params_string != '':
                                params = []
                                for j in params_string.split("##"):
                                    temp_str = re.sub(":(?=[a-zA-Z_\d])", "", j)
                                    params.append({"value": f"{temp_str}"})
                                temp["params_details"] = params
                        except Exception as e:
                            print_error("Error while assigning UDFs. Defaulting to customudf")
                            print_error(str(e))
                            temp["transformation_extension_name"] = "customudf"
                            temp["params_details"] = []
                        print_info(f"Applying function mapping {temp['transformation_function_alias']} for "
                                   f"column {column_name_in_targetdb}")
                        self.logger.debug(f"Applying function mapping {temp['transformation_function_alias']} for "
                                          f"column {column_name_in_targetdb}")
                # Modify the input sql type as needed to the udfs
                temp["sql_type"] = UDF_INPUT_SQL_TYPE_MAPPING.get(temp.get("transformation_function_alias"),
                                                                  temp["sql_type"])

                # For each column apply nvl transformation to return default outputs
                if not temp["name"].lower().startswith("ziw") and temp.get("transformation_function_alias",
                                                                           "") == "" and not temp.get(
                    "is_excluded_column",
                    False) and self.configurations_json.get("replace_null_with_defaults",False):
                    # This means there is no UDF attached, hence attach a nvl udf
                    temp["transformation_function_alias"] = "nvl"
                    temp["transformation_extension_name"] = SOURCE_EXTENSION_NAME
                    if temp["target_sql_type"] == 12:
                        temp["params_details"] = [{"value": "\"\""}]
                    if temp["target_sql_type"] in [3, 7, 8, 4, -5]:
                        temp["params_details"] = [{"value": column_val_datatype_mapping.get(temp["target_sql_type"])}]
                    elif temp["target_sql_type"] == 91:
                        temp["params_details"] = [{"value": "to_date(\"01/01/1990\",\"MM/dd/yyyy\")"}]
                    elif temp["target_sql_type"] == 93:
                        temp["params_details"] = [
                            {"value": "to_timestamp(\"01/01/1990 00:00:00\",\"MM/dd/yyyy HH:mm:ss\")"}]

                columns_to_add.append(temp)

            # Add new columns
            iwx_columns_list = [item["name"].upper() for item in columns_to_add if
                                not item.get("is_excluded_column", False)]
            new_columns_to_add = \
                self.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')['NEW_COLUMNS'].tolist()[
                    0]
            if new_columns_to_add:
                new_columns_to_add = json.loads(new_columns_to_add)
                for column_data in new_columns_to_add:
                    if list(column_data.keys())[0] in iwx_columns_list:
                        continue
                    for k, v in column_data.items():
                        column_type = 12
                        iwx_columns_list.append(k.upper())
                        if v.lower().startswith('lit'):
                            if v.lower().strip() == "lit(current_timestamp(0))" or v.lower().strip() == "lit(current_date)":
                                temp = {"column_type": "target", "sql_type": 93, "is_deleted": False,
                                        "name": k, "original_name": k,
                                        "target_sql_type": 93, "is_audit_column": False,
                                        "transformation_extension_name": SOURCE_EXTENSION_NAME,
                                        "transformation_function_alias": "nvl",
                                        "params_details": [{"value":"current_timestamp()"}],
                                        "format": "yyyy-MM-dd HH:mm:ss.SSS"}
                            elif v.lower().strip() == "lit(date)":
                                temp = {"column_type": "target", "sql_type": 91, "is_deleted": False,
                                        "name": k, "original_name": k,
                                        "target_sql_type": 91, "is_audit_column": False,
                                        "transformation_extension_name": SOURCE_EXTENSION_NAME,
                                        "transformation_function_alias": "nvl",
                                        "params_details": [{"value":"current_date()"}],
                                        "format": "yyyy-MM-dd"}
                            else:
                                column_props = {"column_type": "target", "sql_type": column_type,
                                                "is_deleted": False, "name": k,
                                                "original_name": k, "target_sql_type": column_type,
                                                "is_audit_column": False,
                                                "transformation_extension_name": SOURCE_EXTENSION_NAME,
                                                "transformation_function_alias": "nvl"}
                                column_val = v.replace("lit(", "").replace("LIT(", "").rstrip(")")
                                if is_int(column_val):
                                    column_props["sql_type"] = 4
                                    column_props["target_sql_type"] = 4
                                    column_props["transformation_function_alias"] = "nvl"
                                    column_props["params_details"] = [{"value": column_val}]
                                elif is_float(column_val):
                                    # precision,scale = str(column_val).split(".")
                                    column_props["sql_type"] = 7
                                    column_props["target_sql_type"] = 7
                                    column_props["transformation_function_alias"] = "nvl"
                                    column_props["params_details"] = [{"value": column_val}]
                                elif column_val.lower().replace(" ", "").replace("'", "") == "||@load_batch_id||":
                                    column_props["sql_type"] = 3
                                    column_props["transformation_function_alias"] = "nvl"
                                    column_props["target_sql_type"] = 3
                                    column_props["target_precision"] = "38"
                                    column_props["target_scale"] = "0"
                                    column_val = "1234567890"
                                else:
                                    column_props["sql_type"] = 12
                                    column_props["target_sql_type"] = 12
                                    column_props["transformation_function_alias"] = "nvl"
                                column_props["params_details"] = [{"value": column_val}]
                                temp = column_props
                        elif v.lower() == "current_date":
                            temp = {"column_type": "target", "sql_type": 91, "is_deleted": False,
                                    "name": k, "original_name": k,
                                    "target_sql_type": 91, "is_audit_column": False,
                                    "transformation_extension_name": SOURCE_EXTENSION_NAME,
                                    "transformation_function_alias": "nvl",
                                    "params_details": [{"value":"current_date()"}]}
                        elif v.lower() == "current_time":
                            temp = {"column_type": "source", "sql_type": 12, "is_deleted": False,
                                    "name": k, "original_name": k,
                                    "target_sql_type": 12, "is_audit_column": False,
                                    "transformation_extension_name": SOURCE_EXTENSION_NAME,
                                    "transformation_function_alias": "nvl",
                                    "params_details": [{"value":"date_format(current_timestamp(), 'HH:mm:ss')"}]}
                        elif v.lower().startswith("nvl2"):  # To handle nvl extract
                            nvl_contents = v.replace("nvl2(", "").replace("$$)", "")
                            params = []
                            for j in nvl_contents.split("##"):
                                temp_str = re.sub(":(?=[a-zA-Z_\d])", "", j)
                                params.append({"value": f'{temp_str}'})
                            temp = {"column_type": "target", "sql_type": 12, "is_deleted": False, "name": k,
                                    "original_name": k, "target_sql_type": 12, "is_audit_column": False,
                                    "transformation_extension_name": SOURCE_EXTENSION_NAME,
                                    "transformation_function_alias": "nvl2", "params_details": params}
                        elif v.lower().startswith("nvl"):  # To handle nvl extract
                            nvl_contents = v.replace("nvl(", "").replace("$$)", "")
                            params = []
                            for j in nvl_contents.split("##"):
                                temp_str = re.sub(":(?=[a-zA-Z_\d])", "", j)
                                params.append({"value": f'{temp_str}'})
                            temp = {"column_type": "target", "sql_type": 12, "is_deleted": False, "name": k,
                                    "original_name": k, "target_sql_type": 12, "is_audit_column": False,
                                    "transformation_extension_name": SOURCE_EXTENSION_NAME,
                                    "transformation_function_alias": "nvl", "params_details": params}
                        else:
                            try:
                                transformation_function_alias, params_string = list(
                                    re.match("(.*)\((.*)\)", v).groups())
                                params = []
                                for j in params_string.split("##"):
                                    temp_str = re.sub(":(?=[a-zA-Z_\d])", "", j)
                                    params.append({"value": f'{temp_str}'})
                                temp = {"column_type": "target", "sql_type": 12, "is_deleted": False, "name": k,
                                        "original_name": k, "target_sql_type": 12, "is_audit_column": False,
                                        "transformation_extension_name": SOURCE_EXTENSION_NAME,
                                        "transformation_function_alias": transformation_function_alias,
                                        "params_details": params
                                        }
                            except Exception as e:
                                temp = {"column_type": "target", "sql_type": 12, "is_deleted": False, "name": k,
                                        "original_name": k, "target_sql_type": 12, "is_audit_column": False,
                                        "transformation_extension_name": SOURCE_EXTENSION_NAME,
                                        "transformation_function_alias": "nvl",
                                        "params_details": [{"value": f"{v.strip().strip(':')}"}]
                                        }

                        col_snowflake_lookup_details: dict = snowflake_columns_details.get(k, None)
                        if col_snowflake_lookup_details is not None:
                            if col_snowflake_lookup_details.get("target_sql_type", ""):
                                temp["target_sql_type"] = col_snowflake_lookup_details["target_sql_type"]
                            if col_snowflake_lookup_details.get("target_precision", ""):
                                temp["target_precision"] = col_snowflake_lookup_details["target_precision"]
                            if col_snowflake_lookup_details.get("target_scale", ""):
                                temp["target_scale"] = col_snowflake_lookup_details["target_scale"]
                            if col_snowflake_lookup_details.get("col_size", ""):
                                temp["col_size"] = col_snowflake_lookup_details["col_size"]
                        columns_to_add.append(temp)
                        self.logger.debug(f"Added new column {k} for table {table_name}")
            # Add columns present only in snowflake and not in Infoworks
            missing_columns = list(set(snowflake_columns_list) - set(iwx_columns_list))
            for item in missing_columns:
                column_value = column_mappings.get(item, None)
                col_detail_in_sf = snowflake_columns_details.get(item.upper(), {})
                temp_dict = {"column_type": "target", "sql_type": 12, "is_deleted": False,
                             "name": item.upper(), "original_name": item.upper(),
                             "target_sql_type": col_detail_in_sf.get("target_sql_type", 12),
                             "is_audit_column": False}
                if col_detail_in_sf.get("target_precision", ""):
                    temp_dict["target_precision"] = col_detail_in_sf["target_precision"]
                if col_detail_in_sf.get("target_scale", ""):
                    temp_dict["target_scale"] = col_detail_in_sf["target_scale"]
                if col_detail_in_sf.get("col_size", ""):
                    temp_dict["col_size"] = col_detail_in_sf["col_size"]
                if temp_dict["target_sql_type"] == 91:
                    temp_dict["format"] = "yyyy-MM-dd"
                if temp_dict["target_sql_type"] == 93:
                    temp_dict["format"] = "yyyy-MM-dd HH:mm:ss.SSS"
                if temp_dict["target_sql_type"] in [12, 3, 4, -5, 7, 8]:
                    temp_dict["transformation_extension_name"] = SOURCE_EXTENSION_NAME
                    temp_dict["transformation_function_alias"] = "nvl"
                    temp_dict["params_details"] = [
                        {"value": column_val_datatype_mapping.get(temp_dict["target_sql_type"]) if self.configurations_json.get("replace_null_with_defaults",False) else column_value}]

                if temp_dict["target_sql_type"] == 93:
                    temp_dict["transformation_extension_name"] = SOURCE_EXTENSION_NAME
                    temp_dict["transformation_function_alias"] = "nvl"
                    temp_dict["params_details"] = [
                        {"value": column_val_datatype_mapping.get(temp_dict["target_sql_type"]) if self.configurations_json.get("replace_null_with_defaults",False) else column_value}]

                if item.upper() == "DW_LOAD_DT" or temp_dict["target_sql_type"] == 91:
                    temp_dict["transformation_extension_name"] = SOURCE_EXTENSION_NAME
                    temp_dict["transformation_function_alias"] = "nvl"
                    temp_dict["params_details"] = [{"value":"current_date()"}]
                    temp_dict["sql_type"]=91
                if item.upper() == "DW_LOAD_TM":
                    temp_dict["transformation_extension_name"] = SOURCE_EXTENSION_NAME
                    temp_dict["transformation_function_alias"] = "nvl"
                    temp_dict["params_details"] = [{"value":"date_format(current_timestamp(), 'HH:mm:ss')"}]
                    temp_dict["sql_type"] = 93
                if column_value is not None and "||" in column_value:
                    params = [{"value": item.strip(":")} for item in
                              column_value.replace("'", "\"").strip("(").strip(")").strip().split("||")]
                    temp_dict["transformation_extension_name"] = SOURCE_EXTENSION_NAME
                    temp_dict["transformation_function_alias"] = "concat"
                    temp_dict["params_details"] = params
                elif column_value is not None:
                    temp_dict["transformation_extension_name"] = SOURCE_EXTENSION_NAME
                    temp_dict["transformation_function_alias"] = "nvl"
                    temp_dict["params_details"] = [{"value": column_value.strip(":")}]

                columns_to_add.append(temp_dict)
            self.logger.debug(f"Added missing columns for table {table_name}" + " , ".join(missing_columns))

            # TO-DO Update the table configurations

            client_result = self.iwx_client.get_table_configurations(source_id, table_id)
            if client_result.get("result")["status"] == "success":
                table_configs = client_result.get("result")["response"]["result"]
                table_configs["last_ingested_cdc_value"] = None
                table_configs["configuration"]["write_supported_engines"] = ["SNOWFLAKE", "SPARK"]
                table_configs["configuration"]["read_supported_engines"] = ["SNOWFLAKE", "SPARK"]
                table_configs["configuration"]["is_scd2_table"] = False
                table_configs["configuration"]["natural_keys"] = []
                table_configs["configuration"]["storage_format"] = "delta"
                table_configs["configuration"]["sync_type"] = "full-load"
                table_configs["configuration"]["exclude_legacy_audit_columns"] = True
                table_configs["configuration"]["generate_history_view"] = False
                ziw_cols = [{
                    'column_type': 'target',
                    'sql_type': 12,
                    'is_deleted': False,
                    'name': 'ziw_file_name',
                    'original_name': 'ziw_file_name',
                    'target_sql_type': 12,
                    'is_audit_column': True,
                    'is_excluded_column': True},
                    {'column_type': 'target',
                     'sql_type': 93,
                     'is_deleted': False,
                     'name': 'ziw_file_modified_timestamp',
                     'original_name': 'ziw_file_modified_timestamp',
                     'target_sql_type': 93,
                     'is_audit_column': True,
                     'is_excluded_column': True,
                     'format': 'yyyy-MM-dd HH:mm:ss'},
                    {'column_type': 'target',
                     'sql_type': 93,
                     'is_deleted': False,
                     'name': 'ziw_target_timestamp',
                     'original_name': 'ziw_target_timestamp',
                     'target_sql_type': 93,
                     'is_audit_column': True,
                     'is_excluded_column': True,
                     'target_scale': '6',
                     'precision': 0,
                     'target_precision': '0',
                     'scale': 6,
                     'format': 'yyyy-MM-dd HH:mm:ss'},
                    {'column_type': 'target',
                     'sql_type': 16,
                     'is_deleted': False,
                     'name': 'ziw_is_deleted',
                     'original_name': 'ziw_is_deleted',
                     'target_sql_type': 16,
                     'is_audit_column': True,
                     'is_excluded_column': True,
                     'target_scale': '0',
                     'precision': 0,
                     'target_precision': '0',
                     'scale': 0}]
                columns_to_add.extend(ziw_cols)
                table_configs["columns"] = columns_to_add
                self.iwx_client.update_table_configuration(source_id, table_id, table_configs)

        except Exception as e:
            traceback.print_exc()
            logging.error(str(e))
            raise
        print_success(f"Configured table {table_name} successfully")


def parallelized_file_mapping_configuration(obj: FixedWidthSource, table, source_id, snowflake_source_id,
                                            reconfigure_tables_flag):
    table_name = table.split("/")[-1]
    sf_table_name = table_name
    try:
        sf_table_name = obj.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')['TARGET_TABLE_NAME'].tolist()[
        0]
    except (IndexError,KeyError) as e:
        pass
    LRF_Layout = obj.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')['LRF Layout'].tolist()[
        0]
    LRF_Path = obj.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')['LRF Path'].tolist()[0]
    LRF_Name = obj.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')['LRF Name'].tolist()[0]
    if LRF_Layout is not None and LRF_Path is not None and LRF_Name is not None and validate_lrf_layout(LRF_Layout):
        file_mappings_body = {"configuration": {}}
        file_mappings_body["configuration"]["source_file_properties"] = obj.default_file_properties.copy()
        file_mappings_body["configuration"]["source_file_properties"]["columns_configurations"] = [
            {"column_width": int(i.strip())} for i in LRF_Layout.split("\n")[0].split(",")]
        file_mappings_body["configuration"]["target_relative_path"] = f"/{sf_table_name}_schema"
        file_mappings_body["configuration"]["deltaLake_table_name"] = f"{sf_table_name}"
        file_mappings_body["configuration"]["source_file_type"] = "fixedwidth"
        file_mappings_body["configuration"]["ingest_subdirectories"] = False
        table_path = "/{table}"
        try:
            table_path = \
                obj.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')['LRF Path'].tolist()[0]
            if '.' not in table_path.split("/")[-1]:
                table_path = table_path + "/" if not table_path.endswith("/") else table_path
            else:
                dir_name, file_name_ext = os.path.split(table_path)
                table_path = dir_name

            #replace the files to files_archive in the LRF path
            if obj.configurations_json.get("replace_files_to_archive_in_path",True):
                table_path=table_path.replace("/files/","/files_archive/")

        except (KeyError, IndexError) as e:
            traceback.print_exc()
            print_warning(str(e))
            print_warning(
                f"Did not find the LRF path for {table_name} in table schema csv. Going with default path.")
            obj.logger.info(
                f"Did not find the LRF path for {table_name} in table schema csv. Going with default path.")
        file_mappings_body["configuration"]["source_relative_path"] = table_path
        file_mappings_body["configuration"]["exclude_filename_regex"] = "(.*done_bkp.*|.*done_backup.*|.*_bkp)"
        LRF_Names = obj.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')['LRF Name'].tolist()[
            0]
        if LRF_Names.split("\n")[0] == "":
            # file_mappings_body["configuration"]["include_filename_regex"] = ".*"
            if "_" in table_name:
                tbl_name_pattern = table_name.split("_")[0].split("#")[0].upper()
                file_mappings_body["configuration"][
                    "include_filename_regex"] = f".*({tbl_name_pattern}|{tbl_name_pattern.lower()}).*(.dat|.DAT)$"
            else:
                file_mappings_body["configuration"]["include_filename_regex"] = ".*"
        else:
            LRF_Names = list(filter(lambda x: x != '', LRF_Names.split("\n")))
            LRF_Names = [f".*{i.strip().replace('.DAT', '.dat')}(.gz)?" for i in LRF_Names]
            file_mappings_body["configuration"]["include_filename_regex"] = f'({"|".join(LRF_Names)})'

        file_mappings_body["configuration"]["is_archive_enabled"] = False
        table_schema = ""
        try:

            table_schema = \
                obj.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')['DB Name'].tolist()[0]
            #table_schema = table_schema.replace("EDW", "SDW_ECDW_").replace("DB", "_DB").strip()
            table_schema = schema_lookup.get(table_schema.upper(), table_schema)
            try:
                table_schema = \
                obj.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')['TARGET_SCHEMA_NAME'].tolist()[0]
            except (KeyError, IndexError) as e:
                # traceback.print_exc()
                print(str(e))
                print(
                    f"Did not find the TARGET_SCHEMA_NAME for {table_name} in table schema csv.Going with default schema from configuration.json.")
                obj.logger.info(
                    f"Did not find the TARGET_SCHEMA_NAME for {table_name} in table schema csv.Going with default schema from configuration.json.")
        except (KeyError, IndexError) as e:
            traceback.print_exc()
            print_warning(str(e))
            print_warning(
                f"Did not find the DB Name for {table_name} in table schema csv. Going with default schema from "
                f"configuration.json.")
            obj.logger.info(
                f"Did not find the DB Name for {table_name} in table schema csv. Going with default schema from "
                f"configuration.json.")
            try:
                table_schema = \
                obj.table_schema_df.query(f"TABLE_NAME_JOIN_COL == '{table_name}'").fillna('')['TARGET_SCHEMA_NAME'].tolist()[0]
            except (KeyError, IndexError) as e:
                # traceback.print_exc()
                print(str(e))
                print(
                    f"Did not find the TARGET_SCHEMA_NAME for {table_name} in table schema csv.Going with default schema from configuration.json.")
                obj.logger.info(
                    f"Did not find the TARGET_SCHEMA_NAME for {table_name} in table schema csv.Going with default schema from configuration.json.")
        file_mappings_body["configuration"][
            "target_schema_name"] = table_schema if table_schema and table_schema.strip().lower() != "na" else \
            obj.snowflake_default_props["sfSchema"]
        file_mappings_body["configuration"]["target_table_name"] = f"{sf_table_name}"
        file_mappings_body["configuration"]["target_database_name"] = obj.snowflake_default_props["sfDatabase"]
        file_mappings_body["configuration"]["is_table_case_sensitive"] = False
        file_mappings_body["configuration"]["is_schema_case_sensitive"] = False
        file_mappings_body["configuration"]["is_database_case_sensitive"] = False
        file_mappings_body["name"] = f"{sf_table_name}"
        file_mappings_body["source"] = source_id
        file_mappings_body["meta_crawl_performed"] = True
        file_mappings_body_serialised = json.dumps(file_mappings_body.copy())
        obj.logger.debug(f"File Mappings for table: {table_name} " + json.dumps(file_mappings_body))
        table_id = obj.configure_file_mappings(source_id, file_mappings_body_serialised,
                                               reconfigure_tables=reconfigure_tables_flag)

        # Configure the table
        try:
            obj.configure_tables(source_id, table_id, table_name, file_mappings_body["configuration"],
                                 snowflake_source_id)
        except Exception as e:
            obj.skipped_tables.append((table_name, "Configuration of table skipped: " + str(e), None, None))
            return f"{table_name} Configuration of table skipped"
        return f"{table_name} File Mappings done"
    else:
        obj.skipped_tables.append((
            table_name, f"LRF_Layout or LRF_Path or LRF_Name is None or LRF_Layout is invalid", None,
            None))
        return f"{table_name} File Mappings skipped"


def main():
    parser = argparse.ArgumentParser('Fixed Width Source automation')
    parser.add_argument('--source_name', required=True, help='Pass the name of Fixed Width source to be created')
    parser.add_argument('--environment_name', required=False, default="",
                        help='Pass the name of the environment that source should use')
    parser.add_argument('--environment_storage_name', required=False, default="",
                        help='Pass the name of the environment storage that source should use')
    parser.add_argument('--environment_compute_name', required=False, default="",
                        help='Pass the name of the environment compute that source should use')
    parser.add_argument('--create_source_extension', required=False, default="False",
                        help='Pass True to create the source extension', choices=["True", "False"])
    parser.add_argument('--snowflake_metasync_source_name', required=False,
                        help='Pass name of snowflake metasync source in Infoworks')
    parser.add_argument('--configuration_json_file', required=False, default="./conf/configurations.json",
                        help='Pass the path to configuration json file including filename')
    parser.add_argument('--reconfigure_tables_flag', required=False, default="False",
                        help='Pass True if you want to '
                             'reconfigure the table '
                             'configurations if table '
                             'with name is already '
                             'created ion the source',
                        choices=["True",
                                 "False"])
    parser.add_argument('--table_schema_path', required=True, help='Pass the full path of the file with table schema '
                                                                   'details')
    args = vars(parser.parse_args())
    configuration_json_path = args.get("configuration_json_file")
    configuration_json = json.load(open(configuration_json_path, "r"))
    host = configuration_json.get('host', 'localhost')
    port = configuration_json.get('port', '3001')
    protocol = configuration_json.get('protocol', 'http')
    refresh_token = configuration_json.get("refresh_token", "")
    if refresh_token == "":
        print("Please pass the refresh_token variable in configuration.json file.Exiting..")
        exit(-100)
    src_obj = FixedWidthSource(refresh_token=refresh_token, host=host, port=port, protocol=protocol)
    src_obj.configurations_json = configuration_json
    src_obj.snowflake_default_props["sfDatabase"] = configuration_json.get("sfDatabase", "DEFAULT_DB")
    src_obj.snowflake_default_props["sfSchema"] = configuration_json.get("sfSchema", "PUBLIC")
    source_name = args.get("source_name")
    environment_name = args.get("environment_name", "")
    environment_storage_name = args.get("environment_storage_name", "")
    environment_compute_name = args.get("environment_compute_name", "")
    snowflake_source_name = args.get("snowflake_metasync_source_name", None)
    get_sourceid_from_name_response = src_obj.iwx_client.get_sourceid_from_name(snowflake_source_name)
    snowflake_source_id = get_sourceid_from_name_response.get("result",{}).get("response",{}).get("id",None)
    if snowflake_source_id is None:
        print("Source id could not be found for given snowflake source name.Please validate and rerun the script.Exiting...")
        exit(-100)
    table_schema_path = args.get("table_schema_path")
    src_obj.set_table_df(table_schema_path)

    create_source_extension_bool = eval(args.get("create_source_extension", "True"))
    reconfigure_tables_flag_bool = eval(args.get("reconfigure_tables_flag", "False"))
    environment_id = ""
    environment_storage_id = ""
    environment_compute_id = ""
    if configuration_json.get("environment_id","")!="" and configuration_json.get("environment_storage_id","")!="" and configuration_json.get("environment_compute_id","")!="":
        environment_id = configuration_json.get("environment_id","")
        environment_storage_id = configuration_json.get("environment_storage_id","")
        environment_compute_id = configuration_json.get("environment_compute_id","")
    else:
        if not environment_name:
            environment_name = configuration_json["environment_name"]
            if environment_name:
                print_info(f"picking default environment name from configurations.json {environment_name}")
                src_obj.logger.info(f"picking default environment name from configurations.json {environment_name}")
            else:
                print_error("Could not find the environment_name in configurations.json Please verify the same. Exiting...")
                src_obj.logger.error("Could not find the environment_name in configurations.json Please verify the same. "
                                     "Exiting...")
                sys.exit(-100)
            if not environment_storage_name:
                environment_storage_name = configuration_json["environment_storage_name"]
                if environment_storage_name:
                    print_info(f"picking default environment storage name from configurations.json {environment_storage_name}")
                    src_obj.logger.info(
                        f"picking default environment storage name from configurations.json {environment_storage_name}")
                else:
                    print_error(
                        "Could not find the environment_storage_name in configurations.json. Please verify the same. Exiting...")
                    src_obj.logger.error("Could not find the environment_storage_name in configurations.json. Please verify "
                                         "the same. Exiting...")
                    sys.exit(-100)
            if not environment_compute_name:
                environment_compute_name = configuration_json["environment_compute_name"]
                if environment_compute_name:
                    print_info(f"picking default environment compute name from configurations.json {environment_compute_name}")
                    src_obj.logger.info(
                        f"picking default environment compute name from configurations.json {environment_compute_name}")
                else:
                    print_error(
                        "Could not find the environment_compute_name in configurations.json. Please verify the same. Exiting...")
                    src_obj.logger.error("Could not find the environment_compute_name in configurations.json. Please verify "
                                         "the same. Exiting...")
                    sys.exit(-100)

            print_info("environment_name : " + environment_name)
            print_info("environment_compute_name : " + environment_compute_name)
            print_info("environment_storage_name : " + environment_storage_name)

        get_environment_id_from_name_response = src_obj.iwx_client.get_environment_id_from_name(environment_name)
        environment_id = get_environment_id_from_name_response.get("result",{}).get("response",{}).get("environment_id",None)
        get_storage_id_from_name_response = src_obj.iwx_client.get_storage_id_from_name(environment_id, environment_storage_name)
        environment_storage_id = get_storage_id_from_name_response.get("result",{}).get("response",{}).get("storage_id",None)
        get_compute_id_from_name_response = src_obj.iwx_client.get_compute_id_from_name(environment_id, environment_compute_name)
        environment_compute_id = get_compute_id_from_name_response.get("result",{}).get("response",{}).get("compute_id",None)
    print_info(f"Got environment_id {environment_id}")
    src_obj.logger.info(f"Got environment_id {environment_id}")
    print_info(f"Got environment_storage_id {environment_storage_id}")
    src_obj.logger.info(f"Got environment_storage_id {environment_storage_id}")
    print_info(f"Got environment_compute_id {environment_compute_id}")
    src_obj.logger.info(f"Got environment_compute_id {environment_compute_id}")
    if configuration_json.get("source_extensions_to_add"):
        source_extension = configuration_json.get("source_extensions_to_add", ["att_source_extensions"])[0]
    else:
        source_extension = "fixed_width_att_source_extensions"

    if create_source_extension_bool:
        key, advance_config_body = create_src_extensions(src_obj.iwx_client)
    use_staging_schema_for_infoworks_managed_tables_bool = configuration_json.get("use_staging_schema_for_infoworks_managed_tables",True)
    source_creation_body = {"name": source_name, "environment_id": environment_id, "storage_id": environment_storage_id,
                            "data_lake_path": f"/iw/sources/{source_name}",
                            "data_lake_schema": configuration_json.get("sfSchema", "PUBLIC"),
                            "staging_schema_name": configuration_json.get("sfStageSchema", "PUBLIC"),
                            "target_database_name": configuration_json.get("sfDatabase", "CSV_AUTOMATED"),
                            "is_database_case_sensitive": False, "is_schema_case_sensitive": False, "type": "file",
                            "sub_type": "fixedwidth", "transformation_extensions": [source_extension],
                            "use_staging_schema_for_infoworks_managed_tables": use_staging_schema_for_infoworks_managed_tables_bool,
                            "is_source_ingested": True}
    src_obj.logger.debug("source creation body: " + json.dumps(source_creation_body))
    source_id = src_obj.create_source(source_creation_body)
    src_obj.logger.info(f"source id : {source_id}")
    set_src_advanced_config(src_obj.iwx_client, source_id)
    if create_source_extension_bool:
        set_src_advanced_config(src_obj.iwx_client, source_id, (key, advance_config_body))
    src_obj.logger.info("Added advanced configs to the source")

    # prepare the source connection body
    sftp_base_path = configuration_json.get("sftp_details", {}).get("sftp_base_path", '')
    if not sftp_base_path.startswith("/"):
        sftp_base_path = "/" + sftp_base_path
    if sftp_base_path != "/":
        sftp_base_path = sftp_base_path.rstrip("/")
    if not sftp_base_path:
        print_error("Please ensure to provide the sftp_base_path config in configurations.json. Exiting...")
        src_obj.logger.info("Please ensure to provide the sftp_base_path config in configurations.json. Exiting...")
    source_connection_details_body = {"source_base_path_relative": sftp_base_path, "source_base_path": ""}
    sftp_username = configuration_json.get("sftp_details", {}).get("sftp_username", '')
    sftp_private_key_path = configuration_json.get("sftp_details", {}).get("sftp_private_key_path", "")
    sftp_private_key = configuration_json.get("sftp_details", {}).get("sftp_private_key", "")
    sftp_secret_name = configuration_json.get("sftp_details", {}).get("sftp_secret_name", "")
    sftp_host = configuration_json.get("sftp_details", {}).get("sftp_host", '')
    sftp_port = int(configuration_json.get("sftp_details", {}).get("sftp_port", 22))
    source_connection_details_body["storage"] = {
        "storage_type": "remote",
        "auth_type": "private_key",
        "sftp_host": sftp_host,
        "sftp_port": sftp_port,
        "username": sftp_username
    }
    if sftp_private_key_path!="":
        source_connection_details_body["storage"]["credential"] = {
        "type": "path",
        "private_key_path": sftp_private_key_path
        }
    elif sftp_private_key != "":
        source_connection_details_body["storage"]["credential"] = {
            "type": "text",
            "passphrase": sftp_private_key
        }
    elif sftp_secret_name != "":
        filter_cond = '?filter={"name":"'+sftp_secret_name+'"}'
        secret_id = None
        sftp_secret_name_res = src_obj.iwx_client.call_api(method="GET",
            url= f"""{src_obj.iwx_client.client_config.get('protocol')}://{src_obj.iwx_client.client_config.get('ip')}:{src_obj.iwx_client.client_config.get('port')}/v3/admin/manage-secrets/secrets"""+filter_cond,headers={"Authorization" : f"Bearer {src_obj.iwx_client.client_config.get('bearer_token')}"})
        sftp_secret_name_res_json = sftp_secret_name_res.json()
        if sftp_secret_name_res_json.get("result",[])!=[]:
            secret_id_list=sftp_secret_name_res_json.get("result",[])
            if len(secret_id_list)!=0:
                secret_id = secret_id_list[0]["id"]
        else:
            print("Could not find the secret name in Infoworks.Exiting..")
            exit(-100)
        source_connection_details_body["storage"]["credential"] = {
            "type": "text",
            "passphrase": {
                "password_type": "secret_store", #pragma: allowlist secret
                "secret_id": secret_id #pragma: allowlist secret
            }
        }
    else:
        pass
    #default_warehouse = src_obj.iwx_client.get_environment_default_warehouse(environment_id)
    default_warehouse=""
    source_connection_details_body["warehouse"] = configuration_json.get("sfWarehouse", default_warehouse)
    src_obj.logger.info("Source connection body: " + json.dumps(source_connection_details_body))
    configure_source_connection_response = src_obj.configure_source_connection(source_id,
                                                                               source_connection_details_body)
    response_status = configure_source_connection_response["result"]["status"]
    if response_status == "success":
        print("Configured the source with the connection details successfully")
    else:
        print_error("Failed to configure the source connection details")
        print(configure_source_connection_response)
        src_obj.iwx_client.logger.error("Failed to configure the source connection details")
        src_obj.iwx_client.logger.error(configure_source_connection_response)
    tables_list = src_obj.table_schema_df['TABLE_NAME'].to_list()
    src_obj.logger.info(f"List of tables to configure {' , '.join(tables_list)}")
    table_names_list = [i.split(".")[-1] for i in tables_list]
    total_tables_len = len(table_names_list)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for result in executor.map(parallelized_file_mapping_configuration, [src_obj] * total_tables_len,
                                   table_names_list,
                                   [source_id] * total_tables_len, [snowflake_source_id] * total_tables_len,
                                   [reconfigure_tables_flag_bool] * total_tables_len,
                                   ):
            print_info(result)
        executor.shutdown(wait=True)

    dump_table_details(src_obj.iwx_client, source_id, table_names_list, "TABLE_NAME_JOIN_COL", configuration_json,
                       src_obj.table_schema_df, src_obj.skipped_tables)


if __name__ == '__main__':
    main()
