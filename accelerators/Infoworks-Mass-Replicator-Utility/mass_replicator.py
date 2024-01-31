import json
import sys
import pandas as pd
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s — [ %(levelname)s ] — %(message)s",
                    datefmt='%d-%b-%y %H:%M:%S')

sys.path.insert(0, '/Users/sanath.singavarapu/PycharmProjects/infoworks-python-sdk')
from infoworks.sdk.utils import IWUtils
from infoworks.sdk.client import InfoworksClientSDK
import infoworks.sdk.local_configurations


def create_source(source_config):
    try:
        logging.info("Checking if the Source exists")
        list_source_response = iwx_client.get_list_of_replicator_sources(
            params={"filter": {"name": source_config['name']}})
        logging.debug(f"get_list_of_replicator_sources() - Response: {list_source_response}")

        source_result = list_source_response['result']['response']['result']
        if source_result:
            logging.info("Source already exists")
            source_id = source_result[0]['id']
        else:
            logging.info("Source doesn't exist, Creating new Source")
            create_source_response = iwx_client.create_replicator_source(source_config)
            logging.debug(f"create_replicator_source() - Response: {create_source_response}")

            if create_source_response['result']['status'] == "success":
                source_id = create_source_response['result']['response'].get('result', None)
            else:
                raise Exception(f"API Status Returned as Failed")

        return source_id
    except Exception as source_error:
        raise Exception(f"Failed to create/fetch Source: {source_error}")


def create_destination(destination_config):
    try:
        logging.info("Checking if the Destination exists")
        list_destinations_response = iwx_client.get_list_of_replicator_destinations(
            params={"filter": {"name": destination_config['name']}})
        logging.debug(f"get_list_of_replicator_destinations() - Response: {list_destinations_response}")

        destination_result = list_destinations_response['result']['response']['result']
        if destination_result:
            logging.info("Destination already exists")
            destination_id = destination_result[0]['id']
        else:
            logging.info("Destination doesn't exist, Creating new Destination")
            create_destination_response = iwx_client.create_replicator_destination(destination_config)
            logging.debug(f"create_replicator_destination() - Response: {create_destination_response}")

            if create_destination_response['result']['status'] == "success":
                destination_id = create_destination_response['result']['response'].get('result', None)
            else:
                raise Exception(f"API Status Returned as Failed")

        return destination_id
    except Exception as destination_error:
        raise Exception(f"Failed to create/fetch Destination: {destination_error}")


def create_replicator_definition(definition_config):
    try:
        logging.info("Checking if Definition exists")
        list_definitions_response = iwx_client.get_list_of_replicator_definitions(
            definition_config['domain_id'],
            params={"filter": {"name": definition_config['name']}})
        logging.debug(f"get_list_of_replicator_definitions() - Response: {list_definitions_response}")

        definition_result = list_definitions_response['result']['response']['result']
        if definition_result:
            logging.info("Definition already exists")
            definition_id = definition_result[0]['id']
        else:
            logging.info("Definition doesn't exist, Creating new Definition")
            create_definition_response = iwx_client.create_replicator_definition(definition_config)
            logging.debug(f"create_replicator_definition() - Response: {create_definition_response}")

            if create_definition_response['result']['status'] == "success":
                definition_id = create_definition_response['result']['response'].get('result', None)
            else:
                raise Exception(f"API Status Returned as Failed")

        return definition_id
    except Exception as definition_error:
        raise Exception(f"Failed to create/fetch Definition: {definition_error}")


def add_source_and_destination_to_domain(domain_id, source_id, destination_id):
    try:
        logging.info("Adding Source and Destination access to Domain")
        source_access_config = {"entity_details": [{"entity_id": source_id, "schema_filter": ".*"}]}
        destination_access_config = {"entity_details": [{"entity_id": destination_id, "schema_filter": ".*"}]}

        source_access_response = iwx_client.add_replicator_sources_to_domain(domain_id=domain_id,
                                                                             config=source_access_config)
        logging.debug(f"add_replicator_sources_to_domain() - Response: {source_access_response}")

        if source_access_response['result']['status'] == "success":
            logging.info("Source is accessible to domain")
        else:
            raise Exception(f"API Status Returned as Failed")

        destination_access_response = iwx_client.add_replicator_destinations_to_domain(domain_id=domain_id,
                                                                                       config=destination_access_config)
        logging.debug(f"add_replicator_destinations_to_domain() - Response: {destination_access_response}")

        if destination_access_response['result']['status'] == "success":
            logging.info("Destination is accessible to domain")
        else:
            raise Exception(f"API Status Returned as Failed")

    except Exception as access_error:
        raise Exception(f"Failed to check/grant source and destination access to domain: {access_error}")


def crawl_source_metadata(domain_id, source_id):
    try:
        crawl_metadata_response = iwx_client.submit_replication_meta_crawl_job(domain_id=domain_id,
                                                                               replicator_source_id=source_id)
        logging.debug(f"submit_replication_meta_crawl_job() - Response: {crawl_metadata_response}")
        metadata_crawl_job_status = crawl_metadata_response['result']['response'].get('result', {}).get('status')

        if metadata_crawl_job_status == "completed":
            logging.info("Successfully Crawled Source Metadata")
        else:
            raise Exception(f"Job Status Returned as {metadata_crawl_job_status}")
    except Exception as crawl_error:
        raise Exception(f"Failed to Crawl Source Metadata: {crawl_error}")


def add_tables_to_definition(domain_id, source_id, definition_id, replication_tables_file):
    try:
        replication_tables = pd.read_csv(replication_tables_file)
        add_tables_config = {
            "selected_objects": []
        }

        for index, table in replication_tables.iterrows():
            table_response = iwx_client.get_replicator_source_table_id_from_name(source_id=source_id,
                                                                                 schema_name=table['Schema_Name'],
                                                                                 table_name=table['Table_Name'])
            table_id = table_response['result']['response'].get('id')
            table_config = {"id": table_id, "table_name": table['Table_Name'], "schema_name": table['Schema_Name']}
            add_tables_config['selected_objects'].append(table_config)

        add_tables_response = iwx_client.add_tables_to_replicator_definition(domain_id, definition_id,
                                                                             add_tables_config)
        logging.info(f"add_tables_to_replicator_definition() - Response: {add_tables_response}")

    except Exception as add_tables_error:
        raise Exception(f"Failed to add tables to Definition: {add_tables_error}")


def configure_schedule(domain_id, definition_id, schedule_config):
    try:
        schedule_response = iwx_client.create_replication_schedule(domain_id, definition_id, schedule_config)
        logging.debug(f"create_replication_schedule() - Response: {schedule_response}")
        logging.info("Schedule configured successfully")
    except Exception as schedule_error:
        raise Exception(f"Failed to configure scheduler: {schedule_error}")


def trigger_data_replication_job(domain_id, definition_id):
    try:
        data_replication_job_response = iwx_client.submit_replication_data_job(domain_id, definition_id)
        logging.debug(f"create_replication_schedule() - Response: {data_replication_job_response}")

        job_id = data_replication_job_response['result']['response'].get('result', {}).get('id')
        logging.info(f"Data Replication Job Id: {job_id}")

        '''
        data_replication_job_status = data_replication_job_response['result']['response'].get('result', {}).get('status')

        if data_replication_job_status == "completed":
            logging.info("Replication Job successful")
        else:
            raise Exception(f"Job Status Returned as {data_replication_job_status}")
        '''

    except Exception as replication_error:
        raise Exception(f"Failed to trigger/monitor Data Replication Job: {replication_error}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Infoworks Mass Replicator Script')
    parser.add_argument('--replication_config', help='Path to Replication Configuration File (.json)', required=True)
    parser.add_argument('--replication_tables', help='Path to Replication Tables File (.csv)', required=True)
    args = parser.parse_args()

    try:
        logging.info(f"Extracting Replication Configs from {args.replication_config}")
        config_file = open(args.replication_config)
        config = json.load(config_file)
        replicator_instance_config = config['replicator_instance']

        infoworks.sdk.local_configurations.REQUEST_TIMEOUT_IN_SEC = 30
        infoworks.sdk.local_configurations.MAX_RETRIES = 3
        iwx_client = InfoworksClientSDK()
        iwx_client.initialize_client_with_defaults(replicator_instance_config['protocol'],
                                                   replicator_instance_config['host'],
                                                   replicator_instance_config['port'],
                                                   replicator_instance_config['refresh_token'])

        replicator_source_id = create_source(config['source'])
        replicator_destination_id = create_destination(config['destination'])
        replicator_domain_id = config['replication']['definition']['domain_id']
        add_source_and_destination_to_domain(replicator_domain_id, replicator_source_id,
                                             replicator_destination_id)
        config['replication']['definition'].update({"replicator_source_name": config['source']['name'],
                                                    "replicator_destination_name": config['destination']['name'],
                                                    "replicator_source_id": replicator_source_id,
                                                    "replicator_destination_id": replicator_destination_id})

        replicator_definition_id = create_replicator_definition(config['replication']['definition'])

        crawl_source_metadata(replicator_domain_id, replicator_source_id)

        add_tables_to_definition(replicator_domain_id, replicator_source_id, replicator_definition_id,
                                 args.replication_tables)

        if config['replication'].get('schedule'):
            configure_schedule(replicator_domain_id, replicator_definition_id, config['replication']['schedule'])

        if config['replication'].get('trigger_replication_data_job'):
            trigger_data_replication_job(replicator_domain_id, replicator_definition_id)
    except Exception as error:
        logging.error(f"Failed to configure replication: {error}")
