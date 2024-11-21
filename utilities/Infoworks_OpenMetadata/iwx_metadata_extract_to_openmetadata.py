import argparse

import requests
from configparser import ConfigParser

from urlbuilder import get_tabledetails_url, get_bearer_token_url, get_pipeline_version_details, \
    get_pipeline_lineage_url, get_pipeline_url

config = ConfigParser()
config.optionxform = str
config.read('config.ini')
master_pipeline_ids = []
master_sourcetable_ids = []
path = []


def get_bearer_token(client_config, token):
    headers = {'Authorization': 'Basic ' + token, 'Content-Type': 'application/json'}
    response = requests.request("GET", get_bearer_token_url(client_config), headers=headers,
                                verify=False)
    if response is not None:
        delegation_token = response.json().get("result", {}).get("authentication_token")
    else:
        delegation_token = None
        print('Something went wrong, unable to get bearer token')

    return delegation_token


def get_client_config():
    hostname = config.get("api_mappings", "ip")
    port = config.get("api_mappings", "port")
    protocol = config.get("api_mappings", "protocol")
    refresh_token_file = config.get("api_mappings", "refresh_token")
    return protocol, hostname, port, refresh_token_file


def get_query_params_string_from_dict(params=None):
    if not params:
        return ""
    string = "?"
    string = string + "&fields=" + str(params.get('fields')) if params.get('fields') else string
    string = string + "&database=" + str(params.get('database')) if params.get('database') else string
    string = string + "&limit=" + str(params.get('limit')) if params.get('limit') else string
    string = string + "&after=" + str(params.get('after')) if params.get('after') else string
    string = string + "&serviceType=" + str(params.get('serviceType')) if params.get('serviceType') else string
    return string


def get_tables(ip, params=None):
    url = f"http://{ip}:8585/api/v1/tables" + get_query_params_string_from_dict(params)
    tables_list = []
    response = requests.request("GET", url)
    if response.status_code == 200:
        result = response.json()
        tables_list.extend(result["data"])
        while "after" in result["paging"]:
            params["after"] = result["paging"]["after"]
            url = f"http://{ip}:8585/api/v1/tables" + get_query_params_string_from_dict(params)
            response = requests.request("GET", url)
            result = response.json()
            tables_list.extend(result["data"])
    return tables_list


def get_pipelines(ip, params=None):
    url = f"http://{ip}:8585/api/v1/pipelines" + get_query_params_string_from_dict(params)
    pl_list = []
    response = requests.request("GET", url)
    if response.status_code == 200:
        result = response.json()
        pl_list.extend(result["data"])
        while "after" in result["paging"]:
            params["after"] = result["paging"]["after"]
            url = f"http://{ip}:8585/api/v1/pipelines" + get_query_params_string_from_dict(params)
            response = requests.request("GET", url)
            result = response.json()
            pl_list.extend(result["data"])
    return pl_list


def create_pipelines(ip, data):
    url = f"http://{ip}:8585/api/v1/pipelines"
    print(f"Creating/Updating pipeline {data}")
    """
    response = requests.request("PUT", url, data=data)
    if response.status_code == 200:
        print("Pipeline created")
    """


def create_lineage_edge(ip, from_entity, to_entity):
    url = f"http://{ip}:8585/api/v1/lineage"
    data = {
        "description": "string",
        "edge": {
            "description": "string",
            "fromEntity": from_entity,
            "toEntity": to_entity
        }
    }
    print(f"Creating Lineage {data}")
    """
    response = requests.request("PUT", url, data=data)
    if response.status_code == 200:
        print("Lineage Edge created")
    """


def get_iwx_ingestion_path_lineage(client_config, headers, src_id, table_id, src_ds_name, tgt_ds_name):
    url_to_get_table_details = get_tabledetails_url(client_config, src_id, table_id)
    response = requests.request("GET", url_to_get_table_details, headers=headers,
                                verify=False)
    if response.status_code == 200:
        result = response.json().get("result", {})
        target_schema_name = result["configuration"]["target_schema_name"]
        target_table_name = result["configuration"]["target_table_name"]
        orig_db_name = result.get("schema_name_at_source", "csv_source_db")
        orig_table_name = result.get("original_table_name", "")
        orig_catalog_name = result.get("catalog_name", None)
        if orig_catalog_name is not None:
            src_fqdn = f"{src_ds_name}.{orig_catalog_name}.{orig_db_name}.{orig_table_name}"
        else:
            src_fqdn = f"{src_ds_name}.{orig_db_name}.{orig_table_name}"
        tgt_fqdn = f"{tgt_ds_name}.default.{target_schema_name}.{target_table_name}"

        return [src_fqdn, tgt_fqdn]


def get_pipeline_lineage(client_config, headers, domain_id, pipeline_fqdn, pipeline_id, pipeline_version_id):
    url_to_get_pipelineversion_details = get_pipeline_version_details(client_config, domain_id, pipeline_id,
                                                                      pipeline_version_id)
    response = requests.request("GET", url_to_get_pipelineversion_details, headers=headers,
                                verify=False)
    if response.status_code == 200 and len(response.json().get("result", {})) > 0:
        pipeline_details = response.json().get("result", {})
        for node in pipeline_details["model"].get("nodes", []):
            req_dict = pipeline_details["model"]["nodes"][node]
            if req_dict['type'].upper() in ["TARGET", "BIGQUERY_TARGET"]:
                src_node_details = []
                if req_dict['type'].upper() == "TARGET":
                    target_schema_name = req_dict['properties'].get("target_schema_name", "")
                    target_table_name = req_dict['properties'].get("target_table_name", "")
                else:
                    target_schema_name = req_dict['properties'].get("dataset_name", "")
                    target_table_name = req_dict['properties'].get("table_name", "")

                output_columns = [i['name'] for i in req_dict["output_entities"]]
                for column_name in output_columns:
                    # print(column_name)
                    if not column_name.upper().startswith("ZIW"):
                        url_to_get_pipeline_lineage = get_pipeline_lineage_url(client_config, domain_id, pipeline_id,
                                                                               pipeline_version_id, column_name, node)
                        response = requests.request("GET", url_to_get_pipeline_lineage, headers=headers,
                                                    verify=False)
                        if response.status_code == 200 and len(response.json().get("result", [])) > 0:
                            graph = response.json().get("result", [])
                            source_node = graph[-1]
                            if source_node.get("node_type", "").upper() == "SOURCE_TABLE":
                                pl_id = source_node.get("pipeline_id", None)
                                if pl_id is not None and pl_id not in master_pipeline_ids:
                                    master_pipeline_ids.append(pl_id)
                                src_id = source_node.get("source_id", None)
                                table_id = source_node.get("table_id", "")
                                if src_id is not None and f"{src_id}:{table_id}" not in master_sourcetable_ids:
                                    master_sourcetable_ids.append(f"{src_id}:{table_id}")
                                targetdl_table_name = source_node["target_table_name"]
                                targetdl_schema_name = source_node["target_schema_name"]
                                fqdn_src = f"DeltaLake.default.{targetdl_schema_name}.{targetdl_table_name}"
                                if fqdn_src not in src_node_details:
                                    src_node_details.append(fqdn_src)
                                    path_to_add = [fqdn_src, pipeline_fqdn,
                                                   f"DeltaLake.default.{target_schema_name}.{target_table_name}"]
                                    path.append(path_to_add[::])


if __name__ == '__main__':
    protocol, hostname, port, refresh_token = get_client_config()
    parser = argparse.ArgumentParser('Pipeline Lineage Dump to OpenMetadata')
    parser.add_argument('--domain_id', required=False, help='Pass the domain id here')
    parser.add_argument('--pipeline_id', required=False, default=None, help='Pass the pipeline id here')
    parser.add_argument('--source_table_ids', required=False, default=None,
                        help='Pass the comma seperated source_id:table_id combination here. Example: 324516t6:123434')
    args = vars(parser.parse_args())
    client_config = {"protocol": protocol, "ip": hostname, "port": port}
    delegation_token = get_bearer_token(client_config, refresh_token)
    headers = {'Authorization': 'Bearer ' + delegation_token, 'Content-Type': 'application/json'}
    iwx_pipeline_service_id = "b53eb5bc-63cc-4cff-9da9-8fed9072836e"
    domain_id = args["domain_id"]
    pipeline_id = args["pipeline_id"]

    if pipeline_id is not None:
        master_pipeline_ids.append(pipeline_id)
    # Check if there are any more master pipeline ids
    while len(master_pipeline_ids) > 0:
        parent_pipeline_id = master_pipeline_ids.pop()
        response = requests.request("GET", get_pipeline_url(client_config, domain_id, parent_pipeline_id),
                                    headers=headers,
                                    verify=False)
        if response.status_code == 200:
            pipeline_version_id = response.json().get("result").get("active_version_id")
            pipeline_description = response.json().get("result").get("description", "")
            pipeline_name = response.json().get("result").get("name")
            create_pipeline_body = {
                "name": pipeline_name,
                "displayName": pipeline_name,
                "description": f"IWX Pipeline URL: {protocol}://{hostname}:3000/pipeline/{pipeline_id}/overview , {pipeline_description}",
                "service": {
                    "id": iwx_pipeline_service_id,
                    "type": "pipelineService"
                }
            }
            create_pipelines("localhost", create_pipeline_body)
            get_pipeline_lineage(client_config, headers, domain_id, f"Infoworks_Pipeline.{pipeline_name}",
                                 parent_pipeline_id, pipeline_version_id)

    if args["source_table_ids"] is not None and args["source_table_ids"] != "":
        master_sourcetable_ids.extend(args["source_table_ids"].split(","))
    while len(master_sourcetable_ids) > 0:
        src_table_id = master_sourcetable_ids.pop()
        src_id, table_id = src_table_id.split(":")
        path.append(
            get_iwx_ingestion_path_lineage(client_config, headers, src_id, table_id, "SQLServer", "DeltaLake"))

    databases_of_interest = []
    for i in path:
        databases_of_interest.append(".".join(i[0].split(".")[0:2]))
        databases_of_interest.append(".".join(i[-1].split(".")[0:2]))

    openmetadata_tables_dict = {}
    for item in databases_of_interest:
        temp_result = get_tables("localhost", {"database": item, "limit": 10})
        for res in temp_result:
            key = res["fullyQualifiedName"]
            value = {
                "deleted": False,
                "displayName": res["name"],
                "fullyQualifiedName": res["fullyQualifiedName"],
                "href": res["href"],
                "id": res["id"],
                "name": res["name"],
                "type": "table"
            }
            openmetadata_tables_dict[key] = value

    openmetadata_pipelines_dict = {}
    temp_result = get_pipelines("localhost", {"serviceType": "Generic", "limit": 10})
    for res in temp_result:
        key = res["fullyQualifiedName"]
        value = {
            "deleted": False,
            "displayName": res["displayName"],
            "fullyQualifiedName": res["fullyQualifiedName"],
            "href": res["href"],
            "id": res["id"],
            "name": res["name"],
            "type": "pipeline"
        }
        openmetadata_pipelines_dict[key] = value

    for item in path:
        if len(item) == 3:
            # pipeline
            from_entity = openmetadata_tables_dict[item[0]]
            to_entity = openmetadata_pipelines_dict[item[1]]
            create_lineage_edge("localhost", from_entity, to_entity)
            from_entity = openmetadata_pipelines_dict[item[1]]
            to_entity = openmetadata_tables_dict[item[-1]]
            create_lineage_edge("localhost", from_entity, to_entity)
        else:
            # Ingestion
            from_entity = openmetadata_tables_dict[item[0]]
            to_entity = openmetadata_tables_dict[item[1]]
            create_lineage_edge("localhost", from_entity, to_entity)

# --domain_id c5a8c2256c825dd6c605497c --pipeline_id c64c7b8d629428a86c2378af
