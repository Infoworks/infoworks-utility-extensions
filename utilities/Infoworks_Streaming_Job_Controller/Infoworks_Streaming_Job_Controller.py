import json
import argparse
import sys
import warnings

warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
warnings.filterwarnings("ignore")

from infoworks.sdk.utils import IWUtils
from infoworks.sdk.client import InfoworksClientSDK
import infoworks.sdk.local_configurations

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Jobs Controller')

    # Common Parameters between all modes
    shared_configs_parser = argparse.ArgumentParser(add_help=False)
    shared_configs_parser.add_argument('--host', required=True, type=str, help='IP Address of Infoworks VM')
    shared_configs_parser.add_argument('--port', required=True, type=int, help='3001 for http, 443 for https')
    shared_configs_parser.add_argument('--protocol', required=True, type=str, help='http/https')
    shared_configs_parser.add_argument('--refresh_token', required=True, type=str,
                                       help='Refresh Token from Infoworks Account to authenticate')

    subparsers = parser.add_subparsers(help="Mode", dest='operation')

    parser_start = subparsers.add_parser("start", help="To Start Kafka Job", parents=[shared_configs_parser])
    parser_start.add_argument('--source_id', type=str, required=True, help='Infoworks Source ID')
    parser_start.add_argument('--compute_template_id', type=str, required=True, help='Infoworks Compute Template ID')
    parser_start.add_argument('--table_ids', type=str, required=True, help='Infoworks Table IDs (comma separated)')

    parser_start = subparsers.add_parser("stop", help="To Stop Kafka Job", parents=[shared_configs_parser])
    parser_start.add_argument('--source_id', type=str, required=True, help='Infoworks Source ID')
    parser_start.add_argument('--table_ids', type=str, required=True, help='Infoworks Table IDs (comma separated)')

    args = parser.parse_args()

    if args.operation in ('start', 'stop'):
        operation = args.operation
    else:
        print("Provide valid operation [start, stop]")
        sys.exit(-1)
    try:
        # Info works Client SDK Initialization
        infoworks.sdk.local_configurations.REQUEST_TIMEOUT_IN_SEC = 60
        infoworks.sdk.local_configurations.MAX_RETRIES = 3  # Retry configuration, in case of api failure.
        iwx_client = InfoworksClientSDK()
        iwx_client.initialize_client_with_defaults(args.protocol, args.host, args.port, args.refresh_token)

        if args.operation == "start":
            start_kafka_job_url = f'{args.protocol}://{args.host}:{args.port}/v3/sources/{args.source_id}/jobs'
            # Start Kafka Job
            request_body = {
                "job_type": "streaming_start",
                "job_name": f"Kafka_{args.source_id}_Job_Start",
                "compute_template_id": args.compute_template_id,
                "table_ids": [table_id.strip() for table_id in args.table_ids.split(',')]
            }
            start_kafka_job_response = iwx_client.call_api("POST", start_kafka_job_url, IWUtils.get_default_header_for_v3(
                iwx_client.client_config['bearer_token']), request_body)

            if start_kafka_job_response.status_code == 200:
                start_kafka_job_response_parsed = IWUtils.ejson_deserialize(start_kafka_job_response.content)
                if start_kafka_job_response_parsed['result']:
                    print("Successfully submitted start request")
                    for table in start_kafka_job_response_parsed['result']:
                        print(f"Table ID: {table} - Job ID: {start_kafka_job_response_parsed['result'][table]}")
                else:
                    print(f"Response: {json.dumps(start_kafka_job_response_parsed)}")
            else:
                print(f"Status Code returned as {start_kafka_job_response.status_code}")
                print(f"Response: {start_kafka_job_response.content}")

        elif args.operation == "stop":
            # Stop Kafka Job
            stop_kafka_job_url = f'{args.protocol}://{args.host}:{args.port}/v3/sources/{args.source_id}/stop-streaming'
            request_body = {
                "table_ids": [table_id.strip() for table_id in args.table_ids.split(',')]
            }
            stop_kafka_job_response = iwx_client.call_api("POST", stop_kafka_job_url, IWUtils.get_default_header_for_v3(
                iwx_client.client_config['bearer_token']), request_body)

            if stop_kafka_job_response.status_code == 200:
                print("Successfully submitted stop request")
            else:
                print(f"Status Code returned as {stop_kafka_job_response.status_code}")

            print(f"Response: {stop_kafka_job_response.content}")

    except Exception as error:
        print(f"Process Failed: {error}")
