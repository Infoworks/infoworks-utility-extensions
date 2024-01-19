import os
import subprocess
import sys
import traceback

import pkg_resources
import logging
import json
import argparse
import warnings

warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s — [ %(levelname)s ] — %(message)s",
                    datefmt='%d-%b-%y %H:%M:%S',
                    )


def get_all_sources():
    # Fetches all available sources in the environment
    try:
        all_sources = []
        logging.info("Initiating Request to get Sources ")
        sources_response = iwx_client.get_list_of_sources(params={'limit': 50})
        if sources_response['result']['status'] == 'success':
            all_sources = sources_response.get('result', {}).get('response', {}).get("result", {})
            return all_sources
        else:
            raise Exception("SourceInfoFetchException", sources_response)

    except Exception as source_errors:
        logging.error("Failed to retrieve all Sources \nError: {error}".format(error=repr(source_errors)))
        sys.exit(-1)


def get_source_connection_details(source_id):
    # Fetches Source Connection Details
    try:
        connection_details_response = iwx_client.get_source_connection_details(source_id=source_id)
        logging.debug("get_source_connection_details() - Response: {}".format(json.dumps(connection_details_response)))

        if connection_details_response['result']['status'] == 'success':
            connection_url = connection_details_response['result'].get('response', {}).get('result', {}).get(
                'connection_url', '')
            connection_username = connection_details_response['result'].get('response', {}).get('result', {}).get(
                'username', '')
            # Only Snowflake Connector contains Warehouse Key
            snowflake_connection_warehouse = connection_details_response['result'].get('response', {}).get('result',
                                                                                                           {}).get(
                'warehouse', '')
            return connection_url, connection_username, snowflake_connection_warehouse
        else:
            raise Exception("SourceConnectionDetailsFetchException", connection_details_response)
    except Exception as source_connection_error:
        logging.error("Failed to retrieve source connection details \nError: {error}"
                      .format(error=repr(source_connection_error)))
        # sys.exit(-1)


def get_all_tables(source_id):
    # Fetches all available Tables in a Source
    try:
        logging.info("Initiating Request to get all tables in source id : {source_id} ".format(source_id=source_id))
        tables_response = iwx_client.list_tables_in_source(source_id, params={'limit': 50})
        if tables_response.get('result', {})['status'] == 'success':
            all_tables = tables_response.get('result', {}).get('response', {}).get('result', {})
            return all_tables
        else:
            raise Exception("TableInfoFetchException", tables_response)
    except Exception as table_error:
        logging.error("Failed to retrieve all tables in the Source \nError: {error}".format(error=repr(table_error)))
        # sys.exit(-1)


def get_datatype_name_from_id(datatype_id):
    # Converts datatype_id to datatype_name
    try:
        datatype_mapping_dict = {
            '-5': 'BIGINT',
            '-2': 'BYTE',
            '3': 'DECIMAL',
            '4': 'INT',
            '7': 'FLOAT',
            '8': 'DOUBLE',
            '12': 'STRING',
            '16': 'BOOLEAN',
            '91': 'DATE',
            '93': 'TIMESTAMP'
        }
        datatype_name = datatype_mapping_dict.get(datatype_id, 'Other')
        return datatype_name
    except Exception as datatype_error:
        logging.error("Failed to identify datatype name from id \nError: {error}".format(error=repr(datatype_error)))
        raise Exception("DataType Identification Failed")


def extract_table_metadata(result):
    # Converts Tables Response into required format
    table_name = result.get('name')
    try:
        source_schema_name = result.get('schema_name_at_source')
        source_table_name = result.get('original_table_name')
        if result['configuration'].get('include_filename_regex') is not None or \
                result['configuration'].get('exclude_filename_regex') is not None:
            source_file_pattern = "Include Regex: {}, Exclude Regex: {}" \
                .format(result['configuration'].get('include_filename_regex'),
                        result['configuration'].get('exclude_filename_regex'))
        else:
            source_file_pattern = None

        target_columns_and_datatypes = {}
        source_columns_and_datatypes = {}
        for column_info in result.get('columns', []):
            target_columns_and_datatypes[str(column_info.get('name'))] = get_datatype_name_from_id(
                str(column_info.get('target_sql_type')))

            # Ignores is_audit_column = True from source_columns_and_datatypes.
            # If is_audit_column key does not exist, It will be considered as not audit column

            if column_info.get('is_audit_column', False) is False:
                source_columns_and_datatypes[str(column_info.get('original_name'))] = get_datatype_name_from_id(
                    str(column_info.get('sql_type')))

        if result.get('export_configuration'):
            target_type = result['export_configuration'].get('target_type').upper()
            target_configs = result['export_configuration'].get('target_configuration')
            target_database_name = target_configs.get('database_name')
            target_schema_name = target_configs.get('dataset_name', target_configs.get('schema_name'))
            target_table_name = target_configs.get('table_name', target_configs.get('collection_name'))
        else:
            target_type = 'HIVE (INGESTION TARGET)'
            target_database_name = ''
            target_schema_name = result['configuration'].get('target_schema_name')
            target_table_name = result['configuration'].get('target_table_name')

        metadata = {
            'source_schema_name': source_schema_name,
            'source_table_name': source_table_name,
            'target_database_name': target_database_name,
            'target_schema_name': target_schema_name,
            'target_table_name': target_table_name,
            'target_columns_and_datatypes': json.dumps(target_columns_and_datatypes),
            'source_columns_and_datatypes': json.dumps(source_columns_and_datatypes),
            'target_type': target_type,
            'source_file_pattern': source_file_pattern
        }

        return 0, metadata, ''

    except Exception as extract_error:
        logging.error("Failed to extract table metadata for table {name} \nError :{error}"
                      .format(name=table_name, error=extract_error))
        return -1, {}, repr(extract_error)
        # sys.exit(-1)


if __name__ == "__main__":
    required = {'pandas', 'infoworkssdk==4.0a8'}
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed

    if missing:
        # logging.info("Found Missing Libraries, Installing Required")
        # logging.info(missing)
        python = sys.executable
        subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

    import pandas as pd
    import warnings

    warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
    from infoworks.sdk.client import InfoworksClientSDK
    import infoworks.sdk.local_configurations

    try:
        parser = argparse.ArgumentParser(description='Extracts metadata of tables and columns created in Infoworks')
        parser.add_argument('--config_file', required=True, help='Fully qualified path of the configuration file')
        args = parser.parse_args()
        config_file_path = args.config_file
        if not os.path.exists(config_file_path):
            raise Exception(f"{config_file_path} not found")
        with open(config_file_path) as f:
            config = json.load(f)
        # Info works Client SDK Initialization
        infoworks.sdk.local_configurations.REQUEST_TIMEOUT_IN_SEC = 60
        infoworks.sdk.local_configurations.MAX_RETRIES = 3  # Retry configuration, in case of api failure.
        iwx_client = InfoworksClientSDK()
        iwx_client.initialize_client_with_defaults(config.get("protocol", "https"), config.get("host", None),
                                                   config.get("port", 443), config.get("refresh_token", None))

        sources = get_all_sources()
        failed_data = []
        final_data = []

        for source in sources:
            source_name = source.get('name')
            source_type = source.get('type')
            source_sub_type = source.get('sub_type')

            if source['type'] == 'rdbms':
                source_connection_url, source_connection_username, snowflake_warehouse \
                    = get_source_connection_details(source.get('id'))
            else:
                source_connection_url = None
                source_connection_username = None
                snowflake_warehouse = None

            # if source_name == "test_teradata":
            tables = get_all_tables(source.get('id'))

            if tables:
                for table in tables:
                    table_info_row = {'source_name': '', 'source_type': '', 'source_sub_type': '',
                                      'source_connection_url': '', 'snowflake_warehouse': '',
                                      'source_connection_username': '',
                                      'target_database_name': '', 'target_schema_name': '', 'target_table_name': '',
                                      'description': '', 'target_columns_and_datatypes': '', 'tags': '',
                                      'source_file_name': '', 'source_schema_name': '',
                                      'source_table_name': '', 'source_columns_and_datatypes': '',
                                      'target_type': ''}

                    # response = get_table_info(source['id'], table['id'])
                    logging.debug("Table Info: {}".format(json.dumps(table)))
                    status, extracted_metadata, error_msg = extract_table_metadata(table)
                    if status == 0:
                        table_info_row['source_name'] = source_name
                        table_info_row['source_type'] = source_type
                        table_info_row['source_sub_type'] = source_sub_type
                        table_info_row['source_connection_url'] = source_connection_url
                        table_info_row['snowflake_warehouse'] = snowflake_warehouse
                        table_info_row['source_connection_username'] = source_connection_username
                        table_info_row['source_schema_name'] = extracted_metadata.get('source_schema_name')
                        table_info_row['source_table_name'] = extracted_metadata.get('source_table_name')
                        table_info_row['source_columns_and_datatypes'] = \
                            extracted_metadata.get('source_columns_and_datatypes')
                        table_info_row['target_database_name'] = extracted_metadata.get('target_database_name')
                        table_info_row['target_schema_name'] = extracted_metadata.get('target_schema_name')
                        table_info_row['target_table_name'] = extracted_metadata.get('target_table_name')
                        table_info_row['target_columns_and_datatypes'] = \
                            extracted_metadata.get('target_columns_and_datatypes')
                        table_info_row['target_type'] = extracted_metadata.get('target_type')
                        table_info_row['source_file_name'] = extracted_metadata.get('source_file_pattern')

                        final_data.append(table_info_row)
                    else:
                        failed_data.append({'source_name': source_name, 'table_name': table['name'],
                                            'error_message': error_msg})
        if len(final_data) > 0:
            logging.info("Saving Output as TableMetadata.csv ")
            pd.DataFrame(final_data).to_csv("TableMetadata.csv",
                                            columns=['source_name', 'source_type', 'source_sub_type',
                                                     'source_connection_url', 'snowflake_warehouse',
                                                     'source_connection_username',
                                                     'source_schema_name', 'source_table_name',
                                                     'source_columns_and_datatypes',
                                                     'target_type', 'target_database_name', 'target_schema_name',
                                                     'target_table_name',
                                                     'description', 'target_columns_and_datatypes', 'tags',
                                                     'source_file_name',
                                                     ],
                                            index=False)
        if len(failed_data) > 0:
            logging.info("Failed Tables Found, Please check Failed_Tables.csv for more information")
            pd.DataFrame(failed_data).to_csv("Failed_Tables.csv", columns=['source_name', 'table_name',
                                                                           'error_message'],
                                             index=False)

    except Exception as error:
        logging.error("Failed to fetch table metadata \nError: {error}".format(error=repr(error)))
        traceback.print_exc()
