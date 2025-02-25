import json
import base64
import sys
import argparse
from infoworks.sdk.client import InfoworksClientSDK


def main():
    parser = argparse.ArgumentParser(description='SQL to Visual Pipeline Converter')
    parser.add_argument('--refresh_token', required=True, type=str, help='Pass the refresh token')
    parser.add_argument('--protocol', required=True, type=str, help='Protocol for the API calls. http/https')
    parser.add_argument('--host', required=True, type=str, help='Rest API Host')
    parser.add_argument('--port', required=True, type=str, help='Rest API Port')
    parser.add_argument('--domain_id', required=True, type=str, help='Domain Id where'
                                                                     'SQL pipelines need to be converted')
    args = parser.parse_args()
    domain_id = args.domain_id

    iwx_client = InfoworksClientSDK()
    iwx_client.initialize_client_with_defaults(args.protocol, args.host, args.port, args.refresh_token)
    pipelines_response = iwx_client.list_pipelines(domain_id=domain_id)
    all_pipelines = pipelines_response.get('result', {}).get('response', {}).get('result', [])

    for pipeline in all_pipelines:
        try:
            pipeline_id = pipeline.get('id')
            active_version_id = pipeline.get("active_version_id")
            pipeline_version_response = iwx_client.get_pipeline_version_details(domain_id=domain_id,
                                                                                pipeline_id=pipeline_id,
                                                                                pipeline_version_id=active_version_id)
            pipeline_version = pipeline_version_response.get('result', {}).get('response', {}).get('result', {})
            pipeline_version_type = pipeline_version.get('type', 'visual')
            if pipeline_version_type.lower() == "sql":
                sql = pipeline_version.get('query')
                sql_base64_bytes = sql.encode("ascii")
                sql_string_bytes = base64.b64decode(sql_base64_bytes)
                sql_query = sql_string_bytes.decode("ascii")
                sql_query = sql_query.strip().lower()

                if sql_query.startswith("insert") or sql_query.startswith("merge") \
                        or sql_query.startswith("update") or sql_query.startswith("delete"):
                    response = iwx_client.create_pipeline_version(domain_id=domain_id, pipeline_id=pipeline_id,
                                                                  body={"description": "", "tags": {"general": []},
                                                                        "is_active": True, "type": "visual"},
                                                                  params={
                                                                      "source_pipeline_version_id": active_version_id})
                    print(f"Response: {json.dumps(response)}")
                    version_id = response.get('result', {}).get('response', {}).get('result', {}).get('id')
                    print(f"Visual Pipeline created successfully - Version ID: {version_id}")
                else:
                    print("This SQL cannot be converted to Visual Pipeline")
        except Exception as error:
            print(f"Failed to convert sql pipeline into visual pipeline : {error}")


if __name__ == "__main__":
    main()
