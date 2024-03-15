import sys
import json
import logging
import argparse
import warnings
import pandas as pd
import subprocess
required = {'infoworkssdk==4.0a13'}
import pkg_resources
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
from infoworks.sdk.client import InfoworksClientSDK
warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s — [ %(levelname)s ] — %(message)s",
                    datefmt='%d-%b-%y %H:%M:%S')


def create_request_body(workflow_schedule_info):
    try:
        request_body = {
            "schedule_status": "enabled",
            "start_date": workflow_schedule_info['start_date'],
            "start_hour": int(workflow_schedule_info['start_hour']),
            "start_min": int(workflow_schedule_info['start_min'])
        }
        if workflow_schedule_info['repeat_interval_unit'].upper() in (
                'NEVER', 'MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH'):
            request_body['repeat_interval_unit'] = workflow_schedule_info['repeat_interval_unit'].upper()
            if int(workflow_schedule_info['repeat_interval_measure']) >= 1:
                request_body['repeat_interval_measure'] = int(workflow_schedule_info['repeat_interval_measure'])
            else:
                print("repeat_interval_measure should be minimum 1")

            if workflow_schedule_info['repeat_interval_unit'].upper() == 'WEEK':
                if workflow_schedule_info['specified_days']:
                    request_body['specified_days'] = list(map(int, workflow_schedule_info['specified_days'].split(',')))
                else:
                    raise Exception("specified_days is required")

            if workflow_schedule_info['repeat_interval_unit'].upper() == "MONTH":
                if workflow_schedule_info['repeat_on_last_day'].upper() == "TRUE":
                    request_body['repeat_on_last_day'] = "true"
                else:
                    if not workflow_schedule_info['repeat_on_last_day'].upper() == "FALSE":
                        logging.error(f"'repeat_on_last_day' support only True / False. Setting default value 'FALSE'")

                    if workflow_schedule_info['specified_days']:
                        request_body['specified_days'] = list(
                            map(int, workflow_schedule_info['specified_days'].split(',')))
                    else:
                        raise Exception("specified_days is required")

        else:
            logging.error(f"Unrecognized interval unit : {workflow_schedule_info['repeat_interval_unit']}. "
                          f"Setting 'NEVER' as repeat interval unit ")

        if workflow_schedule_info['ends'].upper() == 'TRUE':
            request_body['ends'] = True
            request_body['end_date'] = workflow_schedule_info['end_date']
            request_body['end_hour'] = int(workflow_schedule_info['end_hour'])
            request_body['end_min'] = int(workflow_schedule_info['end_min'])
        elif workflow_schedule_info['ends'].upper() == 'FALSE':
            request_body['ends'] = False
        else:
            logging.error(f"'ends' supports only True / False, Setting default value 'FALSE'")
            request_body['ends'] = False

        if workflow_schedule_info['is_custom_job'].upper() == "TRUE":
            request_body['is_custom_job'] = True
            request_body['custom_job_details'] = eval(workflow_schedule_info['custom_job_details'])
        elif workflow_schedule_info['is_custom_job'].upper() == "FALSE" or workflow_schedule_info['is_custom_job'] is None or workflow_schedule_info['is_custom_job'] == '':
            request_body['is_custom_job'] = False
            pass
        else:
            request_body['is_custom_job'] = False
            logging.error(f"'is_custom_job' supports only True / False")

        return request_body

    except Exception as request_body_error:
        logging.error(f"Failed to generate enable schedule request body: {repr(request_body_error)}")


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Infoworks Schedules Automation Script')
        parser.add_argument('--config_file', required=True, help='Fully qualified path of the configuration file')
        parser.add_argument('--schedules_info_file', help='Path to Schedules Info File (.csv)', required=True)
        args = parser.parse_args()

        config_file = open(args.config_file)
        config = json.load(config_file)
        iwx_client = InfoworksClientSDK()
        iwx_client.initialize_client_with_defaults(config.get("protocol", "https"), config.get("host", None),
                                                   config.get("port", 443), config.get("refresh_token", None))

        schedules_info = pd.read_csv(args.schedules_info_file, dtype=str)
        schedules_info.fillna('', inplace=True)
        for index, row in schedules_info.iterrows():
            if row['source_id'] and row['table_group_id']:
                logging.info(f"Configuring schedule for table group : {row['table_group_id']} - "
                             f"source id : {row['source_id']}")
                schedule_user_response = iwx_client.configure_table_group_schedule_user(row['source_id'],
                                                                                        row['table_group_id'])
                logging.debug(f"configure_table_group_schedule_user() - Response: {json.dumps(schedule_user_response)}")

                if schedule_user_response['result']['response'].get('result', {}).get('id') is None:
                    raise Exception("Failed to configure workflow schedule user")
                schedule_request_body = create_request_body(row)

                logging.debug(f"Request Body: {json.dumps(schedule_request_body)}")
                if schedule_request_body:
                    schedule_response = iwx_client.enable_table_group_schedule(row['source_id'], row['table_group_id'],
                                                                               schedule_request_body)
                    logging.debug(f"enable_table_group_schedule() - Response: {json.dumps(schedule_response)}")

                    if schedule_response['result']['response'].get('result', {}).get('id') is None:
                        raise Exception("Failed to configure table group schedule")

            elif row['domain_id'] and row['workflow_id']:
                logging.info(
                    f"Configuring schedule for workflow : {row['workflow_id']} - domain id : {row['domain_id']}")
                schedule_user_response = iwx_client.update_workflow_schedule_user(row['domain_id'], row['workflow_id'])
                logging.debug(f"update_workflow_schedule_user() - Response: {json.dumps(schedule_user_response)}")

                if schedule_user_response['result']['response'].get('result', {}).get('id') is None:
                    raise Exception("Failed to configure workflow schedule user")
                schedule_request_body = create_request_body(row)

                logging.debug(f"Request Body: {json.dumps(schedule_request_body)}")
                if schedule_request_body:
                    schedule_response = iwx_client.enable_workflow_schedule(row['domain_id'], row['workflow_id'],
                                                                            schedule_request_body)
                    logging.debug(f"enable_workflow_schedule() - Response: {json.dumps(schedule_response)}")

                    if schedule_response['result']['response'].get('result', {}).get('id') is None:
                        raise Exception("Failed to configure workflow schedule")
            else:
                logging.error(f"Row {index + 1}: source_id-table_group_id / domain_id-workflow_id are required")

    except Exception as error:
        print(f"Process Failed : {error}")
