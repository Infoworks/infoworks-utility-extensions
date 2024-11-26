import json
import os
import subprocess
import sys
import traceback

import pkg_resources
import logging
import argparse
import warnings

warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s — [ %(levelname)s ] — %(message)s",
                    datefmt='%d-%b-%y %H:%M:%S',
                    )


def get_pipeline_metadata(iwx_client, domain_names: list, domain_ids: list, pipelines: list, get_all_pipelines=False, should_fetch_column_lineage=False):
    domain_mapping = {}
    errors =[]
    if len(domain_ids) > 0:
        for domain_id in domain_ids:
            try:
                domain_response = iwx_client.get_domain_details(domain_id)
                if domain_response.get("result").get("status") == "success":
                    result = domain_response.get("result").get("response", {}).get("result", {})
                    domain_name = result["name"]
                    domain_mapping[domain_id] = domain_name
            except Exception as e:
                errors.append(str(e))
    elif len(domain_names) > 0:
        for domain in domain_names:
            try:
                domain_id_response = iwx_client.get_domain_id(domain)
                if domain_id_response.get("result").get("status") == "success":
                    domain_id = domain_id_response.get("result").get("response", {}).get("domain_id",None)
                    domain_mapping[domain_id] = domain
            except Exception as e:
                errors.append({"domain_name": domain_mapping[domain_id], "error": str(e)})

    logging.info("Domains of interest: " + str(domain_mapping))
    output = []
    for domain_id in domain_mapping:
        pipeline_response = iwx_client.list_pipelines(domain_id=domain_id)
        pipelines_list = pipeline_response.get("result").get("response", {}).get("result", [])
        if pipeline_response.get("result").get("status") == "success":
            for pipeline in pipelines_list:
                try:
                    pipeline_info_row = {'domain_name': '', 'pipeline_name': '','pipeline_type':'visual', 'number_of_versions': '',
                                         'active_version': '', 'batch_engine': '',
                                         'created_at': '',
                                         'created_by': '',
                                         'modified_at': '',
                                         'modified_by': '',
                                         'description': '', 'environment_name': '', 'storage_name': '', 'compute_name': '',
                                         'tags': '',
                                         'src_tables': '', 'target_schema_name': '',
                                         'target_database_name': '', 'target_table_name': '',
                                         'target_type': '', 'target_base_path': '', 'storage_format': '',
                                         'build_mode': '', 'natural_keys': '', 'partition_keys': '', 'sort_by_columns': '',
                                         'target_columns_and_datatypes': '', 'column_lineage': '',
                                         'derived_expr_if_any': ''}
                    # print(pipeline)
                    pipeline_id = pipeline["id"]
                    if not get_all_pipelines:
                        if len(pipelines) > 0 and pipeline_id not in pipelines:
                            continue
                    pipeline_name = pipeline["name"]
                    pipeline_info_row["pipeline_name"] = pipeline_name
                    pipeline_info_row["domain_name"] = domain_mapping[domain_id]
                    pipeline_info_row["batch_engine"] = pipeline["batch_engine"]
                    pipeline_info_row["created_at"] = pipeline.get("created_at","")
                    pipeline_info_row["created_by"] = pipeline.get("created_by","")
                    # pipeline_info_row["created_by"] = user_mapping.get(pipeline["created_by"], "")
                    pipeline_info_row["modified_at"] = pipeline.get("modified_at","")
                    pipeline_info_row["modified_by"] = pipeline.get("modified_by","")
                    # pipeline_info_row["modified_by"] = user_mapping.get(pipeline["modified_by"], "")

                    pl_details_response = iwx_client.get_pipeline(pipeline_id, domain_id)
                    active_version_id = pl_details_response.get("result")["response"]["result"].get("active_version_id")
                    pipeline_info_row["active_version"] = active_version_id
                    number_of_versions = pl_details_response.get("result")["response"]["result"].get("versions_count")
                    pipeline_info_row["number_of_versions"] = number_of_versions
                    pipeline_info_row["description"] = pl_details_response.get("result")["response"]["result"].get(
                        "description", "")
                    pipeline_info_row["tags"] = ",".join(
                        pl_details_response.get("result")["response"]["result"].get("custom_tags", []))

                    pipeline_config_response = iwx_client.export_pipeline_configurations(pipeline_id=pipeline["id"],
                                                                                         domain_id=domain_id)

                    if pipeline_config_response.get("result").get("status") == "success":
                        pipeline_config = pipeline_config_response.get("result").get("response", {}).get("result", {})
                        iw_mappings = pipeline_config.get("configuration",{}).get("iw_mappings", [])
                        src_tables = []
                        for item in iw_mappings:
                            if item["entity_type"] == "table":
                                if "source_name" in item["recommendation"]:
                                    # This is ingestion target as source table
                                    source_name = item["recommendation"]["source_name"]
                                elif "pipeline_name" in item["recommendation"]:
                                    # This is pipeline target as source table
                                    pl_name = item["recommendation"]["pipeline_name"]
                                    domain_name = item["recommendation"]["domain_name"]
                                    source_name = f"{domain_name}:{pl_name}"
                                else:
                                    source_name = ""
                                source_table_name = item["recommendation"]["table_name"]
                                src_tables.append(f"{source_name}:{source_table_name}")
                        environmentName = pipeline_config.get("configuration",{}).get("entity").get('environment_name', '')
                        storageName = pipeline_config.get("configuration",{}).get("entity").get('storage_name', '')
                        computeName = pipeline_config.get("configuration",{}).get("entity").get('compute_name', '')
                        pipeline_info_row["pipeline_type"] = pipeline_config.get("configuration",{}).get("pipeline_configs", {}).get("type", "visual")
                        pipeline_info_row["environment_name"] = environmentName
                        pipeline_info_row["storage_name"] = storageName
                        pipeline_info_row["compute_name"] = computeName
                        pipeline_info_row["src_tables"] = ",".join(src_tables)
                        if pipeline_config.get("configuration",{}).get("pipeline_configs").get("model",{}).get("nodes", {})=={}:
                            output.append(pipeline_info_row.copy())
                            continue
                        target_details = []
                        for node in pipeline_config.get("configuration",{}).get("pipeline_configs").get("model",{}).get("nodes", []):
                            req_dict = pipeline_config.get("configuration",{}).get("pipeline_configs").get("model",{}).get("nodes",[])
                            if req_dict:
                                req_dict = req_dict[node]
                            else:
                                output.append(pipeline_info_row.copy())
                            if req_dict['type'].upper() in ["TARGET", "BIGQUERY_TARGET", "SNOWFLAKE_TARGET","CUSTOM_TARGET"]:
                                target_type = req_dict['type'].upper()
                                props = req_dict["properties"]
                                target_schema_name = props.get("schema_name", "") if props.get("target_schema_name",
                                                                                               None) is None else props.get(
                                    "target_schema_name")
                                target_table_name = props.get("table_name", "") if props.get("target_table_name",
                                                                                             None) is None else props.get(
                                    "target_table_name")
                                if req_dict['type'].upper() == "BIGQUERY_TARGET":
                                    target_database_name = "" if props.get("dataset_name", None) is None else props.get(
                                        "dataset_name")
                                else:
                                    target_database_name = "" if props.get("database_name", None) is None else props.get(
                                        "database_name")
                                target_base_path = "" if props.get("target_base_path", None) is None else props.get(
                                    "target_base_path")
                                natural_keys = ",".join(props.get("natural_keys", [])) if props.get("natural_key",
                                                                                                    None) is None else ",".join(
                                    props.get("natural_key"))
                                partition_keys = "" if props.get("partition_key", None) is None else ",".join(
                                    props.get("partition_key"))
                                sort_by_columns = "" if props.get("sort_by_columns", None) is None else props.get(
                                    "sort_by_columns")
                                if len(sort_by_columns) > 0:
                                    if type(sort_by_columns[0]) == dict:
                                        sort_by_columns = [i["column_name"] for i in sort_by_columns]
                                sort_by_columns = ",".join(sort_by_columns)
                                build_mode = props.get("build_mode", "") if props.get("sync_type",
                                                                                      None) is None else props.get(
                                    "sync_type")
                                storage_format = "" if props.get("storage_format", None) is None else props.get(
                                    "storage_format")
                                column_info = {}
                                for oe in req_dict["output_entities"]:
                                    if not oe["name"].lower().startswith("ziw"):
                                        column_info[oe["name"]] = oe["entity_attributes"].get("data_type", "")

                                # lineage info
                                output_columns = [i['name'] for i in req_dict["output_entities"]]
                                derived_expr_list = {}
                                column_lineage = {}
                                if should_fetch_column_lineage:
                                    for column_name in output_columns:
                                        # print(column_name)
                                        if not column_name.upper().startswith("ZIW"):
                                            graph_response = iwx_client.get_pipeline_lineage(domain_id, pipeline_id,
                                                                                             active_version_id,
                                                                                             column_name, node)
                                            graph = graph_response["result"]["response"]["result"]
                                            source_node = graph[-1]
                                            if source_node.get("node_type", "").upper() == "SOURCE_TABLE":
                                                src_table_name = source_node.get(
                                                    "target_table_name") if "target_table_name" in source_node else source_node.get(
                                                    "original_table_name", "")
                                                src_schema_name = source_node.get(
                                                    "target_schema_name") if "target_schema_name" in source_node else source_node.get(
                                                    "original_schema_name", "")
                                                if "original_column_name" in source_node["input_port_column"]:
                                                    src_column_name = source_node["input_port_column"]["original_column_name"]
                                                else:
                                                    src_column_name = source_node["input_port_column"]["name"]
                                                column_lineage[
                                                    column_name] = f"{src_schema_name}.{src_table_name}.{src_column_name}"
                                            else:
                                                # This column is derived column
                                                is_derived_column = graph[-1].get("output_port_column", {}).get(
                                                    "is_derived_column",
                                                    False)
                                                if is_derived_column:
                                                    derived_expr = graph[-1].get("output_port_column", {}).get("expression", "")
                                                    node_type = graph[-1].get("node_type", "")
                                                    derived_expr_list[column_name] = f"{node_type}:{derived_expr}"

                                target_details.append(
                                    {"target_type": target_type, "target_schema_name": target_schema_name,
                                     "target_database_name": target_database_name,
                                     "target_table_name": target_table_name,
                                     "target_base_path": target_base_path,
                                     "natural_keys": natural_keys, "build_mode": build_mode,
                                     "partition_keys": partition_keys, "sort_by_columns": sort_by_columns,
                                     "target_columns_and_datatypes": column_info, "column_lineage": column_lineage,
                                     "derived_expr_if_any": derived_expr_list,
                                     "storage_format": storage_format})


                        for target in target_details:
                            for key in ["target_type", "target_schema_name", "target_database_name", "target_table_name",
                                        "target_base_path",
                                        "natural_keys", "partition_keys", "sort_by_columns",
                                        "build_mode", "target_columns_and_datatypes", "column_lineage",
                                        "derived_expr_if_any",
                                        "storage_format"]:
                                pipeline_info_row[key] = target[key]
                            # print(pipeline_info_row)
                            output.append(pipeline_info_row.copy())
                except Exception as e:
                    errors.append({"pipeline_id":pipeline["id"],"pipeline_name":pipeline["name"],"domain_name":domain_mapping[domain_id],"error":str(e)})
    print("errors:",errors)
    return output


if __name__ == "__main__":
    required = {'jwt','pandas', 'infoworkssdk==5.0.6'}
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed

    if missing:
        logging.info("Found Missing Libraries, Installing Required")
        python = sys.executable
        subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

    import pandas as pd
    import warnings
    warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
    from infoworks.sdk.client import InfoworksClientSDK
    import infoworks.sdk.local_configurations

    try:
        parser = argparse.ArgumentParser(description='Extracts metadata of pipelines in Infoworks')
        parser.add_argument('--config_file', required=True, help='Fully qualified path of the configuration file')
        parser.add_argument('--domain_names', required=False, type=str,
                            help='Pass the comma seperated list of domain names from which the pipeline metadata has to be fetched. '
                                 'We need admin refresh token inorder to fetch the domain ids for the given domain names')
        parser.add_argument('--domain_ids', required=False, type=str,
                            help='Pass the comma seperated list of domain id from which the pipeline metadata has to be fetched')
        parser.add_argument('--pipelines', required=False, type=str,
                            help='Pass the comma seperated list of pipeline ids for which the pipeline metadata has to be fetched. Do not pass this filed if you need all the pipeline information under the domain')
        parser.add_argument('--should_fetch_column_lineage', required=False, type=str, default="false",
                            help='Pass true/false. This flag enables/disables capturing of column level lineage for pipelines')
        args = parser.parse_args()
        config_file_path = args.config_file
        if not os.path.exists(config_file_path):
            raise Exception(f"{config_file_path} not found")
        with open(config_file_path) as f:
            config = json.load(f)
        args = parser.parse_args()
        domain_names, domain_ids, pipelines = [], [], []
        if args.domain_names is not None:
            domain_names = args.domain_names.split(",")
        if args.domain_ids is not None:
            domain_ids = args.domain_ids.split(",")
        if args.pipelines is not None:
            pipelines = args.pipelines.split(",")
        if args.should_fetch_column_lineage in ["false", "False", "FALSE", "f", "F", "n", "N"]:
            should_fetch_column_lineage = False
        elif args.should_fetch_column_lineage in ["true", "True", "TRUE", "t", "Y", "y"]:
            should_fetch_column_lineage = True
        else:
            should_fetch_column_lineage = False
        # Infoworks Client SDK Initialization
        infoworks.sdk.local_configurations.REQUEST_TIMEOUT_IN_SEC = 60
        infoworks.sdk.local_configurations.MAX_RETRIES = 3  # Retry configuration, in case of api failure.
        iwx_client = InfoworksClientSDK()
        iwx_client.initialize_client_with_defaults(config.get("protocol", "https"), config.get("host", None),
                                                   config.get("port", 443), config.get("refresh_token", None))
        if not (len(domain_names) > 0 or len(domain_ids) > 0):
            domains = iwx_client.list_domains()
            domains = domains.get("result",{}).get("response",{}).get("result",[])
            domain_ids = [domain["id"] for domain in domains]
        if len(pipelines) > 0:
            get_all_pipelines = False
        else:
            get_all_pipelines = True

        output = get_pipeline_metadata(iwx_client, domain_names=domain_names, domain_ids=domain_ids, pipelines=pipelines, get_all_pipelines=get_all_pipelines, should_fetch_column_lineage=should_fetch_column_lineage)
        if len(output) > 0:
            logging.info("Saving Output as PipelineMetadata.csv ")
            pd.DataFrame(output).to_csv("PipelineMetadata.csv",
                                        columns=['domain_name', 'pipeline_name','pipeline_type', 'number_of_versions', 'active_version',
                                                 'batch_engine', 'created_at','created_by',
                                                 'modified_at',
                                                 'modified_by',
                                                 'description', 'environment_name', 'storage_name', 'compute_name',
                                                 'tags',
                                                 'src_tables', 'target_schema_name', 'target_database_name',
                                                 'target_table_name', 'target_type', 'target_base_path',
                                                 'storage_format',
                                                 'build_mode', 'natural_keys', 'partition_keys', 'sort_by_columns',
                                                 'target_columns_and_datatypes', 'column_lineage',
                                                 'derived_expr_if_any'],
                                        index=False)

    except Exception as error:
        traceback.print_exc()
        logging.error("Failed to fetch table metadata \nError: {error}".format(error=repr(error)))
