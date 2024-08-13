import os
import logging
import sys
import argparse
import json
import warnings
import pandas as pd
import pkg_resources
import subprocess
from collections import defaultdict
warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s — [ %(levelname)s ] — %(message)s",
                    datefmt='%d-%b-%y %H:%M:%S',
                    filename='associate_secrets_to_domains.log'
                    )
logging.getLogger().addHandler(logging.StreamHandler())
secret_name_id_mapping = {}
def prepare_secret_name_to_id_mapping(iwx_client):
    secrets = iwx_client.list_secrets()
    secrets = secrets.get("result", {}).get("response", {}).get("result", [])
    for secret in secrets:
        secret_name_id_mapping[secret["name"]] = secret["id"]

def get_secret_name(iwx_client,secret_id):
    secret_details = iwx_client.list_secrets(params={"filter":{"_id":secret_id}})
    secret_details = secret_details.get("result",{}).get("response",{}).get("result",[])
    if secret_details:
        secret_details = secret_details[0]
        secret_name = secret_details.get("name",secret_id)
        return secret_name

def find_secret_id_in_nested_dict(iwx_client,data):
    for key, value in data.items():
        if isinstance(value, dict):
            # If the value is a dictionary, recurse into it
            secret_id = find_secret_id_in_nested_dict(iwx_client ,value)
            if secret_id:
                return secret_id
        elif key == "secret_id":
            #print(f"Found secret_name: {value}")
            # resolve secret name to ID
            return value

    return None

def associate_secrets_to_domains(iwx_client):
    try:
        errors=[]
        sources = iwx_client.get_list_of_sources()
        sources = sources.get("result",{}).get("response",{}).get("result",[])
        domain_secrets = defaultdict(set)
        domain_secret_names={}
        domain_envs=defaultdict(set)
        res = []
        for source in sources:
            #print(json.dumps(source,indent=4))
            environment_id = source["environment_id"]
            source_connection = iwx_client.get_source_connection_details(source_id=source["id"])
            source_connection = source_connection.get("result",{}).get("response",{}).get("result",[])
            secret_id = find_secret_id_in_nested_dict(iwx_client=iwx_client,data=source_connection)
            if secret_id is not None:
                for domain_id in source.get("associated_domains",[]):
                    domain_secrets[domain_id].add(secret_id)
                    domain_envs[domain_id].add(environment_id)
            tables = iwx_client.list_tables_under_source(source_id=source["id"] ,params={"filter":{"export_configuration.sync_type":{ "$exists": True, "$ne": "DISABLED"},
                                                                                                   "export_configuration.connection.data_connection_id": {
                                                                                                       "$exists": False}
                                                                                                   }})
            tables = tables.get("result",{}).get("response",{}).get("result",[])
            for table in tables:
                #print(table)
                secret_id = find_secret_id_in_nested_dict(iwx_client=iwx_client,data=table["export_configuration"])
                if secret_id is not None:
                    logging.info(f"secret_found:{secret_id}")
                    domain_secrets[domain_id].add(secret_id)
        domains_list_response = iwx_client.list_domains_as_admin()
        domains_list = domains_list_response.get("result",{}).get("response",{}).get("result",[])
        for domain in domains_list:
            domain_id = domain["id"]
            if domain.get(domain["id"],""):
                pipelines = iwx_client.list_pipelines(domain_id=domain_id)
                pipelines = pipelines.get("result",{}).get("response",{}).get("result",[])
                for pipeline in pipelines:
                    domain_envs[domain_id].add(pipeline["environment_id"])

        for k,v in domain_secrets.items():
            domain_update_body={}
            domain_secrets[k]=list(v)
            try:
                domain_name = iwx_client.get_domain_details(domain_id=k)
                domain_name = domain_name.get("result",{}).get("response",{}).get("result",{})
                domain_name = domain_name.get("name",k)
                secret_names = [get_secret_name(iwx_client=iwx_client,secret_id=secret_id) for secret_id in v]
                domain_secret_names[domain_name] = secret_names
                res.append({"domain_name":domain_name,"secrets":secret_names,"environments":domain_envs.get(k,[])})
                domain_update_body["secrets"] = list(v)
                domain_update_body["environment_ids"] = list(domain_envs.get(k,[]))
                if domain_update_body["secrets"] and domain_update_body["environment_ids"]:
                    update_response = iwx_client.update_domain(domain_id = k ,config_body=domain_update_body)
                    update_response = update_response.get("result",{}).get("response",{})
                    logging.info(f"update_response:{update_response}")
                    if update_response.get("iw_code")=="IW10025":
                        logging.info("Trying to fix the error by adding missing secrets...")
                        secret_names_string = update_response.get("details","")
                        pattern = r"Cannot remove secrets in use from domains : ([\w\s,-]+)"
                        # Find all matches
                        import re
                        matches = re.findall(pattern, secret_names_string)
                        logging.info(f"matches:{matches}")
                        extra_secrets = []
                        if matches:
                            extra_secrets = [secret.strip() for secret in re.split(r'[,]+', matches[0])]
                            extra_secrets = [secret for secret in extra_secrets if secret]
                            logging.info(f"extra_secrets:{extra_secrets}")
                            domain_secret_names[domain_name].extend(extra_secrets)
                        extra_secret_ids = [secret_name_id_mapping[secret_name] for secret_name in extra_secrets]
                        domain_update_body["secrets"].extend(extra_secret_ids)
                        domain_update_body["secrets"] = list(set(domain_update_body["secrets"]))
                        new_update_response = iwx_client.update_domain(domain_id=k, config_body=domain_update_body)
                        logging.info(f"new_update_response:{new_update_response}")
            except Exception as e:
                logging.error(str(e))
                errors.append(str(e))
                continue
    except Exception as e:
        logging.error(str(e))
        errors.append(str(e))
    #print(json.dumps(domain_secrets,indent=4))
    #print(json.dumps(domain_secret_names,indent=4))
    df = pd.DataFrame(res,columns=["domain_name","secrets","environments"])
    #print(df)
    cwd = os.getcwd()
    logging.info(f"writing data to csv file : {cwd}/associate_screts_to_domains.csv")
    df.to_csv(f"{cwd}/associate_secrets_to_domains.csv",index=False)
    logging.info("write completed!")
    logging.info("Errors ignored : ")
    for error in errors:
        logging.info(error)
if __name__ == "__main__":
    required = {'pandas', 'infoworkssdk==5.0.3'}
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed
    file_path = os.path.dirname(os.path.realpath(__file__))
    if missing:
        logging.info("Found Missing Libraries, Installing Required")
        python = sys.executable
        subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
    #sys.path.insert(0,"/Users/nitin.bs/PycharmProjects/infoworks-python-sdk/")
    import warnings

    warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
    from infoworks.sdk.client import InfoworksClientSDK
    import infoworks.sdk.local_configurations

    try:
        parser = argparse.ArgumentParser(description='Extracts metadata of pipelines in Infoworks')
        parser.add_argument('--config_file', required=True, help='Fully qualified path of the configuration file')
        args = parser.parse_args()
        config_file_path = args.config_file
        if not os.path.exists(config_file_path):
            raise Exception(f"{config_file_path} not found")
        with open(config_file_path) as f:
            config = json.load(f)
        # Infoworks Client SDK Initialization
        infoworks.sdk.local_configurations.REQUEST_TIMEOUT_IN_SEC = 60
        infoworks.sdk.local_configurations.MAX_RETRIES = 3  # Retry configuration, in case of api failure.
        iwx_client = InfoworksClientSDK()
        iwx_client.initialize_client_with_defaults(config.get("protocol", "https"), config.get("host", None),
                                                   config.get("port", 443), config.get("refresh_token", None))
        prepare_secret_name_to_id_mapping(iwx_client)
        associate_secrets_to_domains(iwx_client)
    except Exception as e:
        logging.error(str(e))
        logging.error(f"Error occured: {str(e)}")
