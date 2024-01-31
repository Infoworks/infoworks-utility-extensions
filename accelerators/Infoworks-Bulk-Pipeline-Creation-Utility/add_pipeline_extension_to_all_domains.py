import argparse
import os
import json
import pkg_resources
import subprocess
import sys
#sys.path.insert(0,"/Users/nitin.bs/PycharmProjects/infoworks-python-sdk/")
required = {'infoworkssdk==4.0a6'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
from infoworks.sdk.client import InfoworksClientSDK
def main():
    parser = argparse.ArgumentParser('Add PL Extn to Domain')
    parser.add_argument('--pipeline_extension_ids', nargs='+',required=True, help='Pass the pipeline extension ids to add to domain space seperated')
    parser.add_argument('--config_json_path', type=str, required=True,
                        help='Pass the absolute path of configuration json file')

    args = parser.parse_args()
    configuration_json = {}
    if os.path.exists(args.config_json_path):
        with open(args.config_json_path, "r") as f:
            configuration_json = json.load(f)
    else:
        print(
            f"Specified configuration json path {args.config_json_path} doesn't exist.Please validate and rerun.Exiting..")
        exit(-100)

    pipeline_extension_ids = []
    if args.pipeline_extension_ids is not None:
        pipeline_extension_ids = args.pipeline_extension_ids
    iwx_client = InfoworksClientSDK()
    iwx_client.initialize_client_with_defaults(configuration_json.get('protocol', 'http'),
                                               configuration_json.get('host', 'localhost'),
                                               configuration_json.get('port', '3001'),
                                               configuration_json.get("refresh_token", ""))

    domain_response = iwx_client.list_domains_as_admin()
    print(domain_response)
    if domain_response["result"]["status"] == "success":
        for domain in domain_response["result"]["response"]["result"]:
            domain_id = domain["id"]
            accessible_pipeline_extensions = domain.get("accessible_pipeline_extensions", [])
            config_body = {
                "entity_ids": list(set(accessible_pipeline_extensions + pipeline_extension_ids))
            }
            add_extn_response = iwx_client.add_update_pipeline_extensions_to_domain(domain_id, config_body=config_body,
                                                                                    action_type="update")
            print(domain["name"] + " : " + json.dumps(add_extn_response.get("result").get("response", {})))


if __name__ == '__main__':
    main()