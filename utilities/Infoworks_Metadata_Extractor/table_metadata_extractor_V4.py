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


class IwxException(Exception):
    def __init__(self, message, iwx_response):
        self.message = message
        self.error_desc = iwx_response['error']['error_desc']
        self.code = iwx_response['result']['response'].get("iw_code")
        self.details = iwx_response['result']['response'].get("details")
        self.help_url = iwx_response['result']['response'].get('help')

        self.final_error = "{message}\nInfoworks Code: {iwx_code},\n" \
                           "Error Description: {error_desc} , Details: {details}, Help: {help}" \
            .format(message=self.message, iwx_code=self.code, error_desc=self.error_desc, details=self.details,
                    help=self.help_url)

    def __str__(self):
        return self.final_error


def get_all_sources():
    # Fetches all available sources in the environment
    try:
        logging.info("Initiating Request to get Sources ")
        sources_response = iwx_client.get_list_of_sources(params={'limit': 50})
        logging.debug("get_list_of_sources() Response: {}".format(json.dumps(sources_response)))

        if sources_response['result']['status'] == 'success':
            all_sources = sources_response.get('result', {}).get('response', {}).get("result")
            return all_sources
        else:
            raise IwxException("SourceInfoFetchException", sources_response)

    except Exception as source_errors:
        logging.error("Failed to retrieve all Sources \nError: {error}".format(error=source_errors))
        sys.exit(-1)


def get_source_connection_details(source_id):
    # Fetches Source Connection Details
    try:
        connection_details_response = iwx_client.get_source_connection_details(source_id=source_id)
        logging.debug("get_source_connection_details() - Response: {}".format(json.dumps(connection_details_response)))

        if connection_details_response['result']['status'] == 'success':
            connection_url = connection_details_response['result']['response'].get('result', {}).get('connection_url')
            connection_username = connection_details_response['result']['response'].get('result', {}).get('username')
            # Only Snowflake Connector contains Warehouse Key
            snowflake_connection_warehouse = connection_details_response['result']['response'] \
                .get('result', {}).get('warehouse')
            # For Snowflake Data Environment
            snowflake_profile = connection_details_response['result']['response'].get('result', {}).get('snowflake_profile')

            return {'connection_url': connection_url,
                    'username': connection_username,
                    'snowflake_warehouse': snowflake_connection_warehouse,
                    'snowflake_profile': snowflake_profile}
        else:
            raise IwxException("SourceConnectionDetailsFetchException", connection_details_response)
    except Exception as source_connection_error:
        logging.error("Failed to retrieve source connection details \nError: {error}"
                      .format(error=source_connection_error))
        return {}
        # sys.exit(-1)


def get_data_environment_details(environment_id):
    # Fetches Data Environment Details
    try:
        environment_details_response = iwx_client.get_environment_details(environment_id=environment_id)
        logging.debug("environment_details_response() - Response: {}".format(json.dumps(environment_details_response)))

        if environment_details_response['result']['status'] == 'success' and\
                environment_details_response['result']['response'].get('result', []):
            return environment_details_response['result']['response'].get('result', {})[0]
        else:
            raise IwxException("EnvironmentDetailsFetchException", environment_details_response)
    except Exception as data_environment_error:
        logging.error("Failed to retrieve data environment details \nError: {error}"
                      .format(error=data_environment_error))


def get_all_tables(source_id):
    # Fetches all available Tables in a Source
    try:
        logging.info("Initiating Request to get all tables in source id : {source_id} ".format(source_id=source_id))
        tables_response = iwx_client.list_tables_in_source(source_id, params={'limit': 50})
        logging.debug("list_tables_in_source() Response: {}".format(json.dumps(tables_response)))

        if tables_response['result']['status'] == 'success':
            all_tables = tables_response['result']['response'].get('result')
            return all_tables
        else:
            raise IwxException("TableInfoFetchException", tables_response)
    except Exception as table_error:
        logging.error("Failed to retrieve all tables in the Source \nError: {error}".format(error=repr(table_error)))
        # sys.exit(-1)


def get_datatype_name_from_id(datatype_id):
    # Converts datatype_id to datatype_name
    try:
        datatype_mapping_dict = {
            '-5': 'BIGINT',
            '-2': 'BYTE',
            '1': 'STRING',
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


def get_table_tags_and_description(source_id, table_id):
    # Fetches Table Tags and Descriptions
    try:
        table_metadata_response = iwx_client.get_table_metadata(source_id=source_id,
                                                                table_id=table_id)
        logging.debug("get_table_metadata() Response: {}".format(json.dumps(table_metadata_response)))

        if table_metadata_response['result']['status'] == 'success':
            tags = table_metadata_response['result']['response'].get('result', {}).get('tags')
            description = table_metadata_response['result']['response'].get('result', {}).get('description')

            return tags, description

        else:
            raise IwxException("TableTagsDescriptionFetchException", table_metadata_response)

    except Exception as table_tags_error:
        logging.error("Failed to get table tags and description \nError: {error}".format(error=table_tags_error))


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

        if result.get('export_configuration') and result.get('export_configuration', {}).get('target_type', '').upper():
            target_type = result['export_configuration'].get('target_type', '').upper()
            target_configs = result['export_configuration'].get('target_configuration')
            target_database_name = target_configs.get('database_name')
            target_schema_name = target_configs.get('dataset_name', target_configs.get('schema_name'))
            target_table_name = target_configs.get('table_name', target_configs.get('collection_name'))
        else:
            target_type = f'{data_environment_type.upper()} (INGESTION)'
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
    required = {'pandas', 'infoworkssdk==4.0a14'}
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed

    if missing:
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
        parser.add_argument('--output_dir', default=None)
        args = parser.parse_args()
        config_file_path = args.config_file
        if args.output_dir:
            output_file = args.output_dir+"/TableMetadata.csv"
            output_failed_file = args.output_dir+"/Failed_Tables.csv"
        else:
            current_directory = os.getcwd()
            output_file = current_directory+"/TableMetadata.csv"
            output_failed_file = current_directory+"/Failed_Tables.csv"

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
        data_environments = {}
        for source in sources:
            source_name = source.get('name')
            source_type = source.get('type')
            source_sub_type = source.get('sub_type')
            source_connection_details = get_source_connection_details(source.get('id'))
            source_connection_url = source_connection_details.get('connection_url')
            source_connection_username = source_connection_details.get('username')
            snowflake_warehouse = source_connection_details.get('snowflake_warehouse')
            snowflake_env_profile = source_connection_details.get('snowflake_profile')

            data_env_id = source.get('environment_id')
            if data_env_id in data_environments.keys():
                environment_details = data_environments[data_env_id]
            else:
                environment_details = get_data_environment_details(data_env_id)
                data_environments[data_env_id] = environment_details
            if environment_details is not None:
                snowflake_profile_username = None
                data_environment_name = environment_details.get('name')
                if environment_details.get('data_warehouse_type'):
                    data_environment_type = environment_details.get('data_warehouse_type')
                else:
                    data_environment_type = environment_details.get('platform')
                if data_environment_type.upper() == "SNOWFLAKE":
                    snowflake_profiles = environment_details.get('data_warehouse_configuration', {}).\
                        get('snowflake_profiles', [])
                    for profile in snowflake_profiles:
                        if profile['name'] == snowflake_env_profile:
                            snowflake_profile_username = profile.get('authentication_properties', {}).get('username')
            else:
                data_environment_name = None
                data_environment_type = None
                snowflake_profile_username = None
            # if source_name == "test_teradata":
            tables = get_all_tables(source.get('id'))

            if tables:
                for table in tables:
                    table_info_row = {'source_name': '', 'source_type': '', 'source_sub_type': '',
                                      'source_connection_url': '', 'snowflake_warehouse': '',
                                      'source_connection_username': '', 'data_environment_name': '',
                                      'data_environment_type': '', 'snowflake_profile': '',
                                      'snowflake_profile_username': '',
                                      'target_database_name': '', 'target_schema_name': '', 'target_table_name': '',
                                      'description': '', 'target_columns_and_datatypes': '', 'tags': '',
                                      'source_file_name': '', 'source_schema_name': '',
                                      'source_table_name': '', 'source_columns_and_datatypes': '',
                                      'target_type': ''}

                    # response = get_table_info(source['id'], table['id'])
                    logging.debug("Table Info: {}".format(json.dumps(table)))
                    status, extracted_metadata, error_msg = extract_table_metadata(table)
                    table_metadata = get_table_tags_and_description(source.get('id'), table['id'])
                    if table_metadata:
                        table_tags, table_description = table_metadata
                        table_tags = ','.join(table_tags)
                    else:
                        table_tags = None
                        table_description = None
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
                        table_info_row['data_environment_name'] = data_environment_name
                        table_info_row['data_environment_type'] = data_environment_type
                        table_info_row['snowflake_profile'] = snowflake_env_profile
                        table_info_row['snowflake_profile_username'] = snowflake_profile_username
                        table_info_row['target_database_name'] = extracted_metadata.get('target_database_name')
                        table_info_row['target_schema_name'] = extracted_metadata.get('target_schema_name')
                        table_info_row['target_table_name'] = extracted_metadata.get('target_table_name')
                        table_info_row['description'] = table_description
                        table_info_row['tags'] = table_tags
                        table_info_row['target_columns_and_datatypes'] = \
                            extracted_metadata.get('target_columns_and_datatypes')
                        table_info_row['target_type'] = extracted_metadata.get('target_type')
                        table_info_row['source_file_name'] = extracted_metadata.get('source_file_pattern')

                        final_data.append(table_info_row)
                    else:
                        failed_data.append({'source_name': source_name, 'table_name': table['name'],
                                            'error_message': error_msg})
        if len(final_data) > 0:
            logging.info(f"Saving Output: {output_file}")
            pd.DataFrame(final_data).to_csv(output_file,
                                            columns=['source_name', 'source_type', 'source_sub_type',
                                                     'source_connection_url', 'snowflake_warehouse',
                                                     'source_connection_username',
                                                     'source_schema_name', 'source_table_name',
                                                     'source_columns_and_datatypes', 'data_environment_name',
                                                     'data_environment_type', 'snowflake_profile',
                                                     'snowflake_profile_username',
                                                     'target_type', 'target_database_name', 'target_schema_name',
                                                     'target_table_name',
                                                     'description', 'target_columns_and_datatypes', 'tags',
                                                     'source_file_name',
                                                     ],
                                            index=False)
        if len(failed_data) > 0:
            logging.info(f"Failed Tables Found, Please check {output_failed_file} for more information")
            pd.DataFrame(failed_data).to_csv(output_failed_file, columns=['source_name', 'table_name',
                                                                           'error_message'],
                                             index=False)

    except Exception as error:
        logging.error("Failed to fetch table metadata \nError: {error}".format(error=repr(error)))
        traceback.print_exc()
        sys.exit(-1)
