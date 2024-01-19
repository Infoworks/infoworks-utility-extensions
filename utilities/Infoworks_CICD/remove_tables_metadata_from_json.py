#Usage:
#python remove_tables_metadata_from_json.py --config_file_path <path to your configurations.json> --exclude_table_names <table names space seperated>

import json
import argparse
from os.path import exists
def main():
    parser = argparse.ArgumentParser(description='Helper utility to remove unnecessary tables metadata from json')
    parser.add_argument('--config_file_path', required=True, type=str, help='Pass the config json file path along with filename')
    parser.add_argument('--exclude_table_names', required=True,nargs='+', type=str, help='Pass the table names whose metadata is to be removed from json')
    parser.add_argument('--update_table_filter_to_empty', required=False, default='true',type=str, help='Pass true if you want to update the table name filter to empty')
    args = parser.parse_args()
    excluded_table_names = args.exclude_table_names
    excluded_table_names = [table.upper() for table in excluded_table_names]
    table_name_id_mappings={}
    table_id_name_mappings = {}
    try:
        if exists(args.config_file_path):
            print(f"Found the file {args.config_file_path}")
        else:
            print(f"Failed to find the file {args.config_file_path}")
    except Exception as e:
        print(str(e))
        exit(-100)
    source_json={}
    with open(args.config_file_path,"r") as f:
        source_json=json.load(f)
    updated_tables=[]
    for table in source_json["configuration"]["table_configs"]:
        if table["configuration"]["name"].upper() not in excluded_table_names:
            updated_tables.append(table)
        else:
            print(f"Removing {table['configuration']['name'].upper()} from table_configs section")
    source_json["configuration"]["table_configs"]=updated_tables
    # remove tables from iw mappings as well
    updated_iw_mappings=[]
    for iw_mapping in source_json["configuration"]["iw_mappings"]:
        if iw_mapping["recommendation"].get("table_name","").upper() not in excluded_table_names:
            updated_iw_mappings.append(iw_mapping)
        else:
            table_id = iw_mapping.get("entity_id","")
            table_name_id_mappings[iw_mapping["recommendation"].get("table_name","").upper()]=table_id
            table_id_name_mappings[table_id]=iw_mapping["recommendation"].get("table_name","").upper()
            print(f"Removing {iw_mapping['recommendation'].get('table_name','').upper()} from iw_mappings section")
    #remove any table group reference
    updated_table_groups = []
    excluded_table_ids = list(table_name_id_mappings.values())
    for tg in source_json["configuration"].get("table_group_configs",[]):
        tables=tg.get("configuration",{}).get("tables",[])
        updated_tables=[]
        for table in tables:
            if table["table_id"] not in excluded_table_ids:
                updated_tables.append(table)
            else:
                print(f"Found {table_id_name_mappings[table['table_id']]} under table group {tg.get('configuration',{}).get('name','')}. Trying to remove this")
        tg["configuration"]["tables"]=updated_tables
        length_of_updated_tables = len(updated_tables)
        for table in updated_tables:
            table["connection_quota"]=100//length_of_updated_tables
    source_json["configuration"]["iw_mappings"]=updated_iw_mappings
    if args.update_table_filter_to_empty.title()=='True':
        if source_json.get("filter_tables_properties",{}):
            source_json["filter_tables_properties"]["tables_filter"]="%"
    with open(args.config_file_path,"w") as f:
        print("Overwriting the existing file with the updated changes after removing table references")
        print(f"Find the file here: {args.config_file_path}")
        json.dump(source_json,f,indent=4)

if __name__ == '__main__':
    main()