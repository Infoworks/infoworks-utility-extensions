import argparse
import sys
import json

from infoworks.sdk.client import InfoworksClientSDK
from configparser import ConfigParser

output = {
    "version": "1.0",
    "tree": [],
    "lineages": []
}
config = ConfigParser()
config.optionxform = str
config.read('config.ini')

from collections import defaultdict


def def_value():
    return {}


def get_client_config():
    hostname = config.get("api_mappings", "ip")
    port = config.get("api_mappings", "port")
    protocol = config.get("api_mappings", "protocol")
    refresh_token_file = config.get("api_mappings", "refresh_token")
    return protocol, hostname, port, refresh_token_file


def pseudo_lineage():
    src_system = {"name": "MySQL", "type": "system", "children": []}
    tgt_system = {"name": "Hive/Spark", "type": "system", "children": []}
    children = []
    for src_db in ["src_db1", "src_db2"]:
        db_children = []
        for src_table_name in ["src_table1", "src_table2"]:
            leaves = []
            for col_name in ["col1", "col2", "col3"]:
                leaves.append({"name": col_name, "type": "column"})
            db_children.append({"name": src_table_name, "type": "table", "leaves": leaves})
        children.append({"name": src_db, "type": "database", "children": db_children})
    src_system["children"] = children
    output["tree"].append(src_system)

    children = []
    for tgt_db in ["tgt_db1", "tgt_db2"]:
        db_children = []
        for tgt_table_name in ["tgt_table1", "tgt_table2"]:
            leaves = []
            for col_name in ["col1", "col2", "col3"]:
                leaves.append({"name": col_name, "type": "column"})
            db_children.append({"name": tgt_table_name, "type": "table", "leaves": leaves})
        children.append({"name": tgt_db, "type": "database", "children": db_children})
    tgt_system["children"] = children
    output["tree"].append(tgt_system)

    for table_details in [{"src_system": "MySQL", "src_database": "src_db1", "src_table": "src_table1",
                           "src_columns": ["col1", "col2", "col3"],
                           "tgt_system": "Hive/Spark", "tgt_database": "tgt_db1", "tgt_table": "tgt_table1",
                           "tgt_columns": ["col1", "col2", "col3"]}]:
        for i, j in zip(table_details["src_columns"], table_details["tgt_columns"]):
            path = {"src_path": [], "trg_path": []}
            path["src_path"].extend(
                [{"system": table_details["src_system"]}, {"database": table_details["src_database"]},
                 {"table": table_details["src_table"]}, {"column": i}])
            path["trg_path"].extend(
                [{"system": table_details["tgt_system"]}, {"database": table_details["tgt_database"]},
                 {"table": table_details["tgt_table"]}, {"column": j}])

            output["lineages"].append(path)

    print(output)


def get_tree_from_dict(system_dict):
    tree = []
    for system in system_dict.keys():
        temp = {"name": system, "type": "system", "children": []}
        values = system_dict.get(system)
        dbs = defaultdict(def_value)  # {'db1': {'table1': ['col1', 'col2'], 'table2': ['col1']}}
        for value in values:
            i, j, k = value.split(".")
            if i in dbs:
                if j in dbs[i]:
                    dbs[i][j].append(k)
                else:
                    dbs[i][j] = [k]
            else:
                dbs[i] = {j: [k]}

        for db in dbs:
            temp_db = {"name": db, "type": "database", "children": []}
            for table in dbs[db]:
                temp_table = {"name": table, "type": "table", "leaves": []}
                columns = dbs[db][table]
                for col_name in columns:
                    temp_table["leaves"].append({
                        'name': col_name,
                        'type': 'column'
                    })
                temp_db["children"].append(temp_table)
            temp["children"].append(temp_db)
        tree.append(temp)
    return tree


def source_lineage(iwx_client_prd: InfoworksClientSDK, source_id, table_ids=None, src_system=None, tgt_system=None):
    src_name = iwx_client_prd.get_sourcename_from_id(source_id)
    # TO-DO after SDK upgrade
    # src_response.get("result")["response"].get("name")
    if src_system is None:
        src_system = iwx_client_prd.get_source_configurations(source_id).get("result", {}).get("response", {}).get(
            "sub_type", "Default")
    if tgt_system is None:
        tgt_system = "HIVE"
    tables_info = []
    if table_ids is None:
        list_tables_response = iwx_client_prd.list_tables_under_source(source_id).get("result", {})
        if list_tables_response["status"] == "success":
            tables_info = list_tables_response.get("response", {})
    else:
        for table_id in table_ids:
            table_response = iwx_client_prd.get_table_info(source_id, table_id)
            if table_response is not None:
                tables_info.append(table_response)

    lineages = []
    system_dict = {}

    for table in tables_info:
        src_schema_name = table.get("schema_name_at_source", src_name)
        src_table_name = table["original_table_name"]
        target_schema_name = table["configuration"]["target_schema_name"]
        target_table_name = table["configuration"]["target_table_name"]
        columns = table["columns"]

        for column in columns:
            column_name = column['name']
            if not column_name.upper().startswith("ZIW"):
                lineages.append({"src_path": [{"system": src_system}, {"database": src_schema_name},
                                              {"table": src_table_name},
                                              {"column": column["original_name"]}],
                                 "trg_path": [{"system": tgt_system}, {"database": target_schema_name},
                                              {"table": target_table_name},
                                              {"column": column["name"]}]})

                if src_system not in system_dict:
                    system_dict[src_system] = [f"{src_schema_name}.{src_table_name}.{column['original_name']}"]
                else:
                    system_dict[src_system].append(f"{src_schema_name}.{src_table_name}.{column['original_name']}")

                # For target system
                if tgt_system not in system_dict:
                    system_dict[tgt_system] = [f"{target_schema_name}.{target_table_name}.{column['name']}"]
                else:
                    system_dict[tgt_system].append(
                        f"{target_schema_name}.{target_table_name}.{column['name']}")

    tree = get_tree_from_dict(system_dict)
    print(json.dumps({'version': '1.0', "tree": tree, 'lineages': lineages}))


def pipeline_lineage(iwx_client_prd: InfoworksClientSDK, pipeline_id, domain_id, master_pipeline_ids,
                     master_sourcetable_ids):
    result = iwx_client_prd.get_pipeline(pipeline_id, domain_id)
    lineages = []
    system_dict = {}
    if result.get("result").get("status") == "success":
        active_version_id = result.get("result")["response"]["result"].get("active_version_id")
        pipeline_name = result.get("result")["response"]["result"].get("name")
        pl_version_details_response = iwx_client_prd.get_pipeline_version_details(pipeline_id, domain_id,
                                                                                  active_version_id)
        pl_version_details = pl_version_details_response["result"]["response"]["result"]
        for node in pl_version_details["model"].get("nodes", []):
            req_dict = pl_version_details["model"]["nodes"][node]
            if req_dict['type'].upper() in ["TARGET", "BIGQUERY_TARGET", "SNOWFLAKE_TARGET"]:
                if req_dict['type'].upper() == "TARGET":
                    target_schema_name = req_dict['properties'].get("target_schema_name", "")
                    target_table_name = req_dict['properties'].get("target_table_name", "")
                    tgt_system = "HIVE"
                elif req_dict['type'].upper() == "BIGQUERY_TARGET":
                    target_schema_name = req_dict['properties'].get("dataset_name", "")
                    target_table_name = req_dict['properties'].get("table_name", "")
                    tgt_system = "BigQuery"
                elif req_dict['type'].upper() == "SNOWFLAKE_TARGET":
                    target_schema_name = req_dict['properties'].get("target_schema_name", "")
                    target_table_name = req_dict['properties'].get("target_table_name", "")
                    tgt_system = "Snowflake"
                else:
                    tgt_system = "HIVE"

                output_columns = [i['name'] for i in req_dict["output_entities"]]

                for column_name in output_columns:
                    # print(column_name)
                    if not column_name.upper().startswith("ZIW"):
                        graph_response = iwx_client_prd.get_pipeline_lineage(domain_id, pipeline_id, active_version_id,
                                                                    column_name, node)
                        graph = graph_response["result"]["response"]["result"]
                        source_node = graph[-1]
                        if source_node.get("node_type", "").upper() == "SOURCE_TABLE":
                            # Based on the node type and environment decide if its CDW or datalake pipelines. Defaulting to hive
                            src_system = "HIVE"
                            pl_id = source_node.get("pipeline_id", None)
                            if pl_id is not None and pl_id not in master_pipeline_ids:
                                master_pipeline_ids.append(pl_id)

                            src_id = source_node.get("source_id", None)
                            src_table_id = source_node.get("table_id", "")
                            if src_id is not None and f"{src_id}:{src_table_id}" not in master_sourcetable_ids:
                                master_sourcetable_ids.append(f"{src_id}:{src_table_id}")

                            src_table_name = source_node["target_table_name"]
                            src_schema_name = source_node["target_schema_name"]
                            src_column_name = source_node["input_port_column"]["name"]

                            lineages.append({"src_path": [{"system": "HIVE"}, {"database": src_schema_name},
                                                          {"table": src_table_name},
                                                          {"column": src_column_name}],
                                             "trg_path": [{"system": tgt_system}, {"database": target_schema_name},
                                                          {"table": target_table_name},
                                                          {"column": column_name}]})

                            # For source system
                            if src_system not in system_dict:
                                system_dict[src_system] = [f"{src_schema_name}.{src_table_name}.{src_column_name}"]
                            else:
                                system_dict[src_system].append(
                                    f"{src_schema_name}.{src_table_name}.{src_column_name}")

                            # For target system
                            if tgt_system not in system_dict:
                                system_dict[tgt_system] = [
                                    f"{target_schema_name}.{target_table_name}.{column_name}"]
                            else:
                                system_dict[tgt_system].append(
                                    f"{target_schema_name}.{target_table_name}.{column_name}")

    tree = get_tree_from_dict(system_dict)
    print(json.dumps({'version': '1.0', "tree": tree, 'lineages': lineages}))
    return master_pipeline_ids, master_sourcetable_ids


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Pipeline Lineage Dump to Collibra')
    parser.add_argument('--domain_id', required=False, help='Pass the domain id here')
    parser.add_argument('--pipeline_id', required=False, default=None, help='Pass the pipeline id here')
    parser.add_argument('--source_id', required=False, default=None, help='Pass the source id here')
    parser.add_argument('--source_table_ids', required=False, default=None,
                        help='Pass the comma seperated source_id:table_id combination here. Example: 324516t6:123434')
    args = vars(parser.parse_args())
    domain_id = args["domain_id"]
    pipeline_id = args["pipeline_id"]
    source_id = args["source_id"]
    protocol, hostname, port, refresh_token = get_client_config()
    iwx_client_prd = InfoworksClientSDK()
    iwx_client_prd.initialize_client_with_defaults(protocol, hostname, port, refresh_token)

    if source_id is not None:
        source_lineage(iwx_client_prd, source_id, None)
    elif pipeline_id is not None:
        master_pipeline_ids, master_sourcetable_ids = pipeline_lineage(iwx_client_prd, pipeline_id, domain_id, [], [])
        while len(master_pipeline_ids) > 0:
            parent_pipeline_id = master_pipeline_ids.pop()
            pipeline_lineage(iwx_client_prd, parent_pipeline_id, domain_id, master_pipeline_ids, master_sourcetable_ids)
        temp_src_dict = {}
        while len(master_sourcetable_ids) > 0:
            src_table_id = master_sourcetable_ids.pop()
            src_id, table_id = src_table_id.split(":")
            if src_id in temp_src_dict:
                temp_src_dict[src_id].append(table_id)
            else:
                temp_src_dict[src_id] = [table_id]

        for src in temp_src_dict:
            source_lineage(iwx_client_prd, src, temp_src_dict[src])


