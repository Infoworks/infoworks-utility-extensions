import os
import logging
import sys
import argparse
import json
import warnings

warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s — [ %(levelname)s ] — %(message)s",
                    datefmt='%d-%b-%y %H:%M:%S',
                    )


def get_source_info(source_id):
    source_response = iwx_client.get_source_configurations(source_id=source_id)
    logging.debug("get_source_configurations() - Response: {}".format(json.dumps(source_response)))
    try:
        # Recheck this again
        if source_response['result']['status'] == 'success':
            environment_id = source_response['result']['response'].get('result', {}).get('environment_id')
            return environment_id
        else:
            raise Exception("SourceInfoFetchException", source_response)
    except Exception as source_response_error:
        logging.error(f"Failed to get source information - Error: {source_response_error}")


def get_pipeline_info(domain_id, pipeline_id):
    pipeline_response = iwx_client.get_pipeline(domain_id=domain_id, pipeline_id=pipeline_id)
    logging.debug("get_pipeline() - Response: {}".format(json.dumps(pipeline_response)))
    try:

        if pipeline_response['result']['status'] == 'success':
            environment_id = pipeline_response['result']['response'].get('result', {}).get('environment_id')
            return environment_id
        else:
            raise Exception("PipelineInfoFetchException", pipeline_response)

    except Exception as pipeline_response_error:
        logging.error(f"Failed to get pipeline information - Error: {pipeline_response_error}")


def get_databricks_workspace_url(environment_id):
    logging.info("Fetching Environment Information")
    environment_response = iwx_client.get_environment_details(environment_id=environment_id)
    logging.debug("get_environment_details() - Response: {}".format(json.dumps(environment_response)))
    try:
        if environment_response['result']['status'] == 'success':
            workspace_url = environment_response['result']['response']['result'][0].get('connection_configuration',
                                                                                        {}).get(
                'workspace_url')
            return workspace_url
        else:
            raise Exception("EnvironmentInfoFetchException", environment_response)
    except Exception as environment_response_error:
        logging.exception(f"Failed to get Environment information - Error: {environment_response_error}")


if __name__ == "__main__":

    import pandas as pd
    import warnings

    warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
    from infoworks.sdk.client import InfoworksClientSDK
    from infoworks.sdk.local_configurations import LOG_LOCATION

    try:
        parser = argparse.ArgumentParser(description='Captures Jobs executed in Infoworks')
        parser.add_argument('--config_file', required=True, help='Fully qualified path of the configuration file')
        parser.add_argument('--time_range_for_jobs_in_mins', required=True, type=str,
                            help='Time Period to fetch the executed jobs')
        parser.add_argument('--output_file', default='JobMetrics.csv')

        args = parser.parse_args()
        config_file_path = args.config_file
        output_file = args.output_file
        if not os.path.exists(config_file_path):
            raise Exception(f"{config_file_path} not found")
        if not output_file.endswith('.csv'):
            raise Exception(f"{output_file} is not of csv type. Only csv is supported")

        with open(config_file_path) as f:
            config = json.load(f)
        iwx_client = InfoworksClientSDK()
        iwx_protocol = config.get("protocol", "https")
        iwx_host = config.get("host", None)
        iwx_port = config.get("port", 443)
        iwx_client.initialize_client_with_defaults(iwx_protocol, iwx_host, iwx_port, config.get("refresh_token", None))

        job_metrics_response = iwx_client.get_abc_job_metrics(
            time_range_for_jobs_in_mins=args.time_range_for_jobs_in_mins)

        if job_metrics_response is not None:
            job_metrics_df = pd.DataFrame(job_metrics_response)
            user_details = iwx_client.get_user_details()
            if user_details['result'].get('response', {}).get('result'):
                user_details_df = pd.DataFrame(user_details['result']['response']['result'])
                user_details_df['email'] = user_details_df['profile'].apply(lambda x: x.get('email', ''))
                user_details_df['name'] = user_details_df['profile'].apply(lambda x: x.get('name', ''))
                user_details_df = user_details_df[["id", "name", "email"]]
                final_df = pd.merge(job_metrics_df, user_details_df, left_on='job_created_by', right_on='id',
                                    how='left') \
                    .drop('id', axis=1)
            else:
                logging.error("Failed to get users list!")
                final_df = job_metrics_df
            log_directory = os.path.dirname(LOG_LOCATION)

            for index, row in final_df.iterrows():
                workspace_url = None
                try:
                    job_info = iwx_client.get_job_details(job_id=row['job_id'])
                    job_info_parsed = job_info.get('result', {}).get('response', {}).get('result', '')

                    entity_type = job_info_parsed[0].get('entity_type')
                    entity_id = job_info_parsed[0].get('entity_id')
                    domain_id = job_info_parsed[0].get('domain_id')
                    if entity_type == 'source':
                        environment_id = get_source_info(entity_id)
                    elif entity_type == 'pipeline':
                        environment_id = get_pipeline_info(domain_id, entity_id)
                    else:
                        environment_id = None
                    if environment_id:
                        workspace_url = get_databricks_workspace_url(environment_id)

                except Exception as error:
                    logging.error(f"Failed to get workspace url \nError: {error}")
                final_df.at[index, 'workspace_url'] = workspace_url
            logging.info(f"Saving Output: {output_file}")

            pd.DataFrame(final_df).to_csv(output_file)

    except Exception as error:
        logging.error("Failed to capture Job Metrics \nError: {error}".format(error=repr(error)))
        sys.exit(-1)
