import re
import os
import argparse
import traceback

import pandas as pd
import json
import csv

import sys
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
pd.set_option('display.max_colwidth', None)
from helper_utilities.utils import is_int, is_float

extension_mapping = {}
with open("conf/extension_mapping.csv", "r") as file:
    csv_file = csv.DictReader(file)
    for row in csv_file:
        extension_mapping[row["parsed_extension"]] = {"iwx_extension_name": row["iwx_extension_name"],
                                                      "search_type": row["search_type"],
                                                      "params": row["params"]}

td_spark_regex = {}
with open("conf/teradata_to_spark_conversion_regex_mapping.csv", "r") as file:
    csv_file = csv.DictReader(file)
    for row in csv_file:
        td_spark_regex[row["td_regex_pattern"]] = row["spark_mapping"]


def find_between(s, start, end):
    try:
        result = (s.split(start))[1].split(end)[0]
        return result
    except IndexError as e:
        print(f"Could not find the statement that starts with {start} and ends with {end}")
        print(str(e))
        return ""


def cleanup_spaces(s):
    s = re.sub("\s+", " ", s).strip()
    if s == " ":
        return ""
    else:
        return s


def get_iwxudf_mapping(inp):
    mapping = {"SYSDATE": "current_date", "SYSDATE4": "current_date", "SYSTIME": "current_time", "DATE": "current_date",
               "TIMESTAMP": "current_time"}
    return mapping.get(inp, None)


def cleanup_insert_sql(insert_sql):
    try:
        col_name = re.search(":[a-zA-Z0-9_]+?(?=(,|\s|\)|\/|\|))", insert_sql + " ").group()
    except:
        col_name = ""
    try:
        pattern = "(.*)\s*\(\s*DATE\s*,\s*FORMAT\s*'(.*)'\)"
        obj = re.search(pattern, insert_sql)
        if obj is not None:
            cleaned_insert_sql = re.sub("\(DATE,\s*FORMAT\s*'[A-Za-z:]+'\)", "", obj.group(1)).strip()
            insert_sql = f"to_date({cleaned_insert_sql}, '{obj.group(2)}')"
    except:
        pass

    cleaned_insert_sql = re.sub("\(DATE,\s*FORMAT\s*'[A-Za-z:]+'\)", "", insert_sql).strip()
    cleaned_insert_sql = re.sub("\(TIMESTAMP\(\d*\)\s*,\s*FORMAT\s*'[A-Za-z:]+'\)", "", cleaned_insert_sql).strip()
    cleaned_insert_sql = re.sub("\(TIMESTAMP\(\d*\)\s*\)", "", cleaned_insert_sql).strip()

    # Regex to replace multiple spaces in the string - TO-DO
    pattern = """\s+(?=(?:[^\'"]*[\'"][^\'"]*[\'"])*[^\'"]*$)"""

    # Find and replace equals and not equals in CASE WHEN statements
    for item in re.findall(
            "((?:WHEN\s*[:(\w,')]+)\s*(?:IS|is|Is|NE|ne|Ne|EQ|eq|Eq|=|<>|!=)?)(\s*'?[\w\s?_@.#&+-]*'?\s*\)?\s*(?:THEN|then|Then))",
            cleaned_insert_sql):
        i, j = item
        if i.strip().upper().endswith("NE"):
            actual_string = i + j
            string_to_replace = i.rstrip('NE').rstrip('ne').rstrip('Ne') + " <> " + j
            cleaned_insert_sql = cleaned_insert_sql.replace(actual_string, string_to_replace)
        elif i.strip().upper().endswith("EQ"):
            actual_string = i + j
            string_to_replace = i.rstrip('EQ').rstrip('eq').rstrip('Eq') + " = " + j
            cleaned_insert_sql = cleaned_insert_sql.replace(actual_string, string_to_replace)
        elif i.endswith("=") or i.endswith("!=") or i.endswith("<>") or i.upper().endswith("IS"):
            pass
        else:
            actual_string = i + j
            string_to_replace = i + " = " + j
            cleaned_insert_sql = cleaned_insert_sql.replace(actual_string, string_to_replace)

    for key in td_spark_regex:
        pattern = key
        replace_value = td_spark_regex[key]
        cleaned_insert_sql = re.sub(pattern, replace_value, cleaned_insert_sql).strip()

    if col_name == cleaned_insert_sql:
        return insert_sql
    else:
        return cleaned_insert_sql


def add_function_mappings(insert_sql):
    insert_sql_orig = insert_sql
    insert_sql = cleanup_insert_sql(insert_sql)
    temp = {}
    flag = False
    for regex_code in extension_mapping.keys():
        regex_match_result = re.search(regex_code, insert_sql_orig.strip().upper())
        if bool(regex_match_result):
            flag = True
            temp["transformation_function_alias"] = extension_mapping.get(regex_code).get(
                "iwx_extension_name")
            search_type = extension_mapping.get(regex_code).get("search_type")
            user_params = extension_mapping.get(regex_code).get("params")
            params_details = []
            if user_params == "":
                if search_type == "single":
                    for i in regex_match_result.groups():
                        if i is None:
                            continue
                        if i.startswith(":"):
                            # params_details.append(f"{i.strip().strip(':')}")
                            params_details.append(f"{i.strip()}")
                        elif i == " ":
                            params_details.append("\" \"")
                        elif is_int(i) or is_float(i):
                            params_details.append(f"{i}")
                        else:
                            params_details.append(f"\"{i.strip().strip(')').strip('(').strip()}\"")
                elif search_type == "recursive":
                    temp_list = re.findall(regex_code, insert_sql_orig.strip().upper())
                    output_list = list(
                        filter(lambda x: x != "", [item for t in temp_list for item in t]))
                    for j in output_list:
                        if j.strip() == "NULL" or j.strip().startswith(":") or bool(
                                re.match(r"\d+", j.strip())):
                            params_details.append(j.strip().strip(":"))
                        elif j == " ":
                            params_details.append("\" \"")
                        elif is_int(j) or is_float(j):
                            params_details.append(f"{j}")
                        else:
                            params_details.append(f"\"{j}\"")
            else:
                for j in user_params.split(","):
                    if j.strip() == "NULL" or j.strip().startswith(":") or bool(
                            re.match(r"\d+", j.strip())):
                        params_details.append(j.strip().strip(":"))
                    else:
                        params_details.append(f"\"{j}\"")

            temp["params_details"] = params_details
            insert_sql = temp["transformation_function_alias"] + "(" + "##".join(temp["params_details"]) + ")"
            print(insert_sql)
            break
    else:
        for ext in ["SYSDATE", "SYSDATE4", "SYSTIME", "DATE", "TIMESTAMP"]:
            if ext in insert_sql and not insert_sql.strip().startswith(":") and not bool(
                    re.search(":[a-zA-Z0-9_]+?(?=(,|\s|\)|\/|\|))", insert_sql.strip())):
                return get_iwxudf_mapping(ext), True
        insert_sql = re.sub("\(DECIMAL\(\d+,\d+\)(.)*\)", "", insert_sql)
        insert_sql = re.sub("\(DECIMAL,\s+FORMAT.*\)", "", insert_sql)
        insert_sql = re.sub("\(CHAR\(\d+\)\)", "", insert_sql)
        col_name = re.search(":.+?(?=(,|\s|\)))", insert_sql + ",")
        if not col_name:
            # return f"lit({insert_sql})", True
            return f"nvl({insert_sql}$$)", True
        # if insert_sql
        insert_sql = re.sub("\(DATE\s*,\s*FORMAT\s*", " to_date(", insert_sql)
        insert_sql = re.sub("\(INTEGER\s*,\s*FORMAT\s*", " to_int(", insert_sql)
        insert_sql = re.sub("\(TIMESTAMP\(\d*\)\s*,\s*FORMAT\s*", " to_timestamp(", insert_sql)
        # Added to handle to_timestamp('YYYY-MM-DDBHH:MI:SS.S(6)') MLCTN302.txt
        insert_sql = insert_sql.replace("YYYY-MM-DDBHH:MI:SS.S(6)", "yyyy-MM-dd HH:mm:ss.SSS'Z'")

        if "FORMAT" in insert_sql.upper():
            flag = True
            if "HH" in insert_sql:
                insert_sql = re.sub("\(FORMAT\s*", " to_timestamp(", insert_sql)
            elif "DATE" in insert_sql.upper():
                insert_sql = re.sub("\(FORMAT\s*", " to_date(", insert_sql)
            else:
                insert_sql = re.sub("\(FORMAT.*", "", insert_sql)
        if bool(re.search("\(TIMESTAMP\(\d*\)\s*", insert_sql)):
            flag = True
            # Has TIMESTAMP(0) or TIMESTAMP(6)
            col_name_val = [i.strip() for i in insert_sql.split(" ",1)][0]
            col_name_val=col_name_val.strip(":")
            if '(6)' in insert_sql:
                str_to_replace = f"""nvl2(to_timestamp({col_name_val},'yyyy-mm-ddbhh:mi:ss.s(6)'),NULL"""
            else:
                str_to_replace = f"""nvl2(to_timestamp({col_name_val},'yyyy-mm-ddbhh:mi:ss.s(0)'),NULL"""

            insert_sql = re.sub("\(TIMESTAMP\(\d*\)\s*", str_to_replace, insert_sql)

    return insert_sql, flag


def removeComments(string):
    string = re.sub(re.compile("/\*.*?\*/", re.DOTALL), "",
                    string)  # remove all occurrences streamed comments (/*COMMENT */) from string
    string = re.sub(re.compile("//.*?\n"), "",
                    string)  # remove all occurrence single-line comments (//COMMENT\n ) from string
    return string


def validate_lrf(row):
    LRF_SCHEMA_LEN = len(row['LRF_SCHEMA'].split(","))
    try:
        LRF_Layout_LEN = len(row["LRF Layout"].split("\n")[0].split(","))
        if LRF_SCHEMA_LEN == LRF_Layout_LEN:
            return True
        else:
            return f"Mismatch SCHEMA : {LRF_SCHEMA_LEN} LAYOUT: {LRF_Layout_LEN}"
    except:
        return False


def main():
    parser = argparse.ArgumentParser('Extract schema from Teradata loader scripts')
    parser.add_argument('--path_to_ctl_files', required=True,
                        help='Pass the absolute path to the directory containing Mload ctl files')
    parser.add_argument('--table_details_csv', required=True,
                        help='Pass the absolute path to the file containing table details')
    args = vars(parser.parse_args())
    folder_path = args.get("path_to_ctl_files")
    cwd = os.getcwd()
    if not os.path.exists(folder_path):
        print("Please enter a valid path to the folder containing the ctl files")
        exit(-100)
    file_flag = False
    if os.path.isdir(folder_path):
        print("\nIt is a directory")
    elif os.path.isfile(folder_path):
        print("\nIt is a normal file")
        file_flag = True

    final_udfs_functions = []
    result = []
    temp = []
    if file_flag:
        files_in_current_directory = [folder_path]
    else:
        files_in_current_directory = os.listdir(folder_path)
    print(files_in_current_directory)
    for filename in files_in_current_directory:
        if filename.endswith(".txt"):
            try:
                need_validation = 'no'
                if file_flag:
                    filename = folder_path
                else:
                    filename = os.path.join(folder_path, filename)
                with open(filename, encoding="utf8", errors='ignore') as f:
                    s = f.read()
                # print(f"Parsing {filename}")

                # Find if there are any delete statements
                table_name_deletes = {}
                if "DELETE FROM" in s:
                    pattern = "(DELETE FROM.*\s+?(WHERE)?.*\s+?;)"
                    temp_match = re.findall(pattern, s)
                    if len(temp_match) > 0:
                        for item in temp_match:
                            table_delete = re.sub("[\n\s+;]", " ", item[0]).strip()
                            t_res = re.search("DELETE FROM(.*)\s+?WHERE\s+?(.*)", table_delete)
                            if t_res is not None:
                                table_name = t_res.group(1).strip()
                                where_clause = t_res.group(2).strip()
                                table_name_deletes[table_name] = where_clause

                mload_entries = []
                if ".BEGIN IMPORT MLOAD" in s:
                    start_entries = s.split(".BEGIN IMPORT MLOAD")[1:]
                    for start_entry in start_entries:
                        mload_entries.append(".BEGIN IMPORT MLOAD " + start_entry.split(".END MLOAD;")[0])
                elif "BEGIN LOADING" in s:
                    start_entries = s.split("BEGIN LOADING")[1:]
                    for start_entry in start_entries:
                        mload_entries.append("BEGIN LOADING " + start_entry.split("END LOADING;")[0])
                else:
                    continue

                for mload_entry in mload_entries:
                    s = mload_entry
                    fastload = False
                    lrf_total_schema_string = find_between(s, ".LAYOUT", ".DML")
                    if lrf_total_schema_string == "":
                        fastload = True
                        lrf_total_schema_string = find_between(s, "DEFINE", "DDNAME")

                    multiple_tables_lrf_definition = lrf_total_schema_string.split("/* FOLLOWING FOR HEADER RECORD */")
                    multiple_tables_lrf_definition = [removeComments(i) for i in multiple_tables_lrf_definition]
                    # multiple_tables_lrf_definition = re.split(r"\/\*.*?\*\/", lrf_total_schema_string)
                    multiple_tables_lrfs = []
                    for i in multiple_tables_lrf_definition:
                        lrf_columns_ordered = []
                        lrf_layout = []
                        for item in i.split("\n"):
                            item = item.strip()
                            if fastload:
                                temp_str_item = item.strip().split("(")[0].strip(",").strip()
                                if temp_str_item != '':
                                    lrf_columns_ordered.append(temp_str_item)
                                    continue
                            if bool(re.search(".FIELD\s+[a-zA-Z0-9_]+\s+[0-9\*]+\s+[A-Za-z0-9]+.*",
                                              item)) or item.startswith(".FILLER") or item.startswith(".FIELD"):
                                item = re.sub("\s+", " ", item)
                                lst = item.split(" ")
                                if "" in lst:
                                    lst.remove("")
                                lrf_columns_ordered.append(lst[1])

                                # Code to handle the fixed width layout
                                lst = [re.sub("\s+", " ", i) for i in lst]
                                if ";" in lst:
                                    lst.remove(";")
                                if "CHAR" in lst[-1]:
                                    lrf_layout.append(
                                        int(lst[-1].strip(";").replace("VARCHAR", "").replace("CHAR", "").replace("(",
                                                                                                                  "").replace(
                                            ")", "")))
                                elif "INTEGER" in lst[-1]:
                                    lrf_layout.append(7)
                                elif "DECIMAL" in lst[-1]:
                                    layout_decimal = lst[-1].strip(";").replace("DECIMAL", "").replace("(", "").replace(
                                        ")",
                                        "").split(
                                        ",")
                                    lrf_layout.append(sum(list(map(int, layout_decimal))))

                        print("lrf_layout",lrf_layout)

                        multiple_tables_lrfs.append((lrf_columns_ordered[::], lrf_layout[::]))
                    table_details_list = []
                    if ".BEGIN IMPORT MLOAD" in s:
                        table_details_list = find_between(s, ".BEGIN IMPORT MLOAD", "WORKTABLES").strip()
                        table_details_list = [item.strip() for item in
                                              re.sub(r'\s+', ' ', table_details_list.replace("\n", " ")).replace(
                                                  "TABLES",
                                                  "").split(
                                                  ",")]

                    if "BEGIN LOADING " in s:
                        table_details_list = find_between(s, "BEGIN LOADING ", ";").strip().split("\n")
                        table_details_list = [table_details_list[0].strip()]

                    for table_name in table_details_list:
                        table = {}
                        column_name_mappings = {}
                        function_mappings = {}
                        additional_columns = []
                        parsing_error = ""
                        try:
                            table_name = table_name.strip()
                            s = re.sub("\s+", " ", s)

                            label_clause = ""
                            if ".DML LABEL" in s:
                                pattern = r"(.DML\s*LABEL)(.+?)(UPDATE\s*" + table_name + r"|INSERT\s*(INTO)*\s*" + table_name + ")"
                                temp_match = re.findall(pattern, s)
                                if len(temp_match) > 0:
                                    label_clause = temp_match[0][1].strip().split(" ")[0].split(";")[0]

                            # Find if there are any selective inserts/updates for the corresponding table
                            selective_clause = ""
                            if ".IMPORT INFILE" in s:
                                pattern = r".IMPORT INFILE DATAIN\s+?.*\s+?.*\s+?;"
                                infile_datains = re.findall(pattern, s)
                                if len(infile_datains) > 0:
                                    for temp_string in infile_datains:
                                        if label_clause != "" and label_clause in temp_string and "WHERE" in temp_string:
                                            selective_clause = temp_string.upper().split("WHERE")[-1].strip(";").strip()
                                            break

                            # print(f"find query : INSERT INTO {table_name}")
                            # print(s)
                            s = re.sub(f"INSERT INTO {table_name} VALUES", f"INSERT INTO {table_name}", s)
                            s = re.sub(f"INSERT {table_name} VALUES", f"INSERT INTO {table_name}", s)
                            s = re.sub(f"INSERT {table_name}", f"INSERT INTO {table_name}", s)
                            insert_sql = find_between(s, f"INSERT INTO {table_name}", ";").strip("\n").strip(" ").strip(
                                "(").strip(
                                ")")
                            if not insert_sql:
                                need_validation = "yes"
                            insert_sql = re.sub("\s", " ", insert_sql)
                            insert_sql = re.sub(":\s+", ":", insert_sql)
                            insert_sql = removeComments(insert_sql)
                            update_sql = find_between(s, f"UPDATE {table_name} SET", ";").strip("\n").strip(" ")
                            natural_keys = []
                            if update_sql:
                                where_clause = re.search('WHERE\s+.*', update_sql).group().strip("WHERE").strip()
                                natural_key_list = where_clause.split("AND")
                                for column_detail in natural_key_list:
                                    column_detail = column_detail.strip().replace("IS", "=")
                                    natural_keys.append(column_detail.split('=')[0].strip())

                            # lrf_col_names_visited => This list is to hold all the lrf columns visited. Incase there is 2nd reference then the target column to be added as addn column
                            # columns_added_as_new_columns => Make sure the column is added either as addn column or have entry in col mapping
                            lrf_col_names_visited = []
                            columns_added_as_new_columns = []
                            for item in re.split(r',(?![^()]*\)|\s*FORMAT\s+)', insert_sql):
                                # print("item:",item)
                                temp_dict = {}
                                teradata_column_name, *lrf_column_details = item.split("=",1)
                                teradata_column_name = teradata_column_name.strip()
                                lrf_column_details = "".join(lrf_column_details)

                                # Find how many column reference are there in the value
                                lrf_column_details_search = lrf_column_details + " "
                                temp_search_result = []
                                while True:
                                    match = re.search(":[a-zA-Z0-9_]+?(?=(,|\s|\(|\)|/|\|))", lrf_column_details_search)
                                    if match:
                                        temp_search_result.append(match.group(0))
                                        lrf_column_details_search = lrf_column_details_search[match.end():]
                                    else:
                                        break
                                temp_search_result = list(set(temp_search_result))
                                if len(temp_search_result) > 0:
                                    lrf_col_name = "".join(temp_search_result)
                                else:
                                    lrf_col_name = None

                                temp_dict["src_column"] = teradata_column_name.strip()
                                lrf_column_details, flag_replaced = add_function_mappings(lrf_column_details.strip())

                                if lrf_col_name is not None and len(temp_search_result) == 1:
                                    if lrf_col_name in lrf_col_names_visited:
                                        temp_str = f'nvl({lrf_column_details.strip().replace(f"{lrf_col_name}", lrf_col_name.strip(":"))})'
                                        additional_columns.append({
                                            teradata_column_name: temp_str})
                                        columns_added_as_new_columns.append(teradata_column_name)
                                    else:
                                        column_name_mappings[teradata_column_name] = lrf_col_name.strip(":")
                                        lrf_col_names_visited.append(lrf_col_name)
                                else:
                                    if lrf_column_details.strip().lower() in ["current_date",
                                                                              "current_time"] or lrf_column_details.strip().lower().startswith(
                                        'lit'):
                                        additional_columns.append({teradata_column_name: re.sub(":(?=[a-zA-Z_\d])", "",
                                                                                                lrf_column_details.strip())})
                                        columns_added_as_new_columns.append(teradata_column_name)
                                    else:
                                        temp_replaced = re.sub(":(?=[a-zA-Z_\d])", "", lrf_column_details.strip())
                                        if not temp_replaced.startswith("nvl"):
                                            additional_columns.append({teradata_column_name: f"nvl({temp_replaced})"})
                                        else:
                                            additional_columns.append({teradata_column_name: f"{temp_replaced}"})
                                        columns_added_as_new_columns.append(teradata_column_name)

                                if lrf_col_name is not None and not lrf_column_details.strip() == lrf_col_name.strip():
                                    try:
                                        col_name_to_remove = re.search(":[a-zA-Z0-9_]+?(?=(,|\s|\)|/|\|))",
                                                                       lrf_column_details + " ").group()
                                    except:
                                        col_name_to_remove = ""

                                    lrf_column_details_replaced_with_empty = re.sub(f":{teradata_column_name}\s*,?", "",
                                                                                    lrf_column_details.strip())
                                    lrf_column_details_replaced_with_empty = re.sub(f"{col_name_to_remove}\s*,?", "",
                                                                                    lrf_column_details_replaced_with_empty)
                                    lrf_column_details_removed_colon = lrf_column_details.strip().replace(
                                        f":{teradata_column_name}", f"{teradata_column_name}").replace(
                                        f"{col_name_to_remove}", f"{col_name_to_remove.strip(':')}")
                                    if teradata_column_name not in columns_added_as_new_columns:
                                        # If the function mapping is matched to one of the regex remove the
                                        # column name from the function
                                        if flag_replaced:
                                            function_mappings[
                                                teradata_column_name] = lrf_column_details_replaced_with_empty
                                        else:
                                            if "to_timestamp" not in lrf_column_details.strip().lower() and "to_date" not in lrf_column_details.strip().lower() and "to_int" not in lrf_column_details.strip().lower():
                                                function_mappings[
                                                    teradata_column_name] = f"nvl2({lrf_column_details_removed_colon},NULL)"
                                            else:
                                                if bool(re.match(".*\(\s*'([a-zA-Z:\/-]+)'\s*\)",
                                                                 lrf_column_details)) or "to_int" in lrf_column_details.strip().lower() or "to_timestamp" in lrf_column_details.strip().lower():
                                                    function_mappings[
                                                        teradata_column_name] = lrf_column_details_replaced_with_empty
                                                else:
                                                    function_mappings[
                                                        teradata_column_name] = f"nvl2({lrf_column_details_removed_colon},NULL)"

                                temp_dict["filename"] = filename
                                print(temp_dict)
                                temp.append(temp_dict)
                                print("column_name_mappings")
                                print(column_name_mappings)
                                print("function_mappings")
                                print(function_mappings)
                                print("natural_keys")
                                print(natural_keys)
                                print("additional_columns")
                                print(additional_columns)
                                print("Need Validation")

                            if len(multiple_tables_lrfs) > 0:
                                lrf_schema_table = []
                                lrf_layout = []
                                for item in multiple_tables_lrfs:
                                    if any(x in item[0] for x in column_name_mappings.values()):
                                        lrf_schema_table = item[0]
                                        lrf_layout = item[-1]
                                        break
                            else:
                                lrf_schema_table = multiple_tables_lrfs[0][0]
                                lrf_layout = multiple_tables_lrfs[0][-1]

                            lrf_schema_file = ','.join(lrf_schema_table)
                            lrf_layout_str = ','.join(list(map(str, lrf_layout)))
                            table["FILE_NAME"] = filename
                            table["TABLE_NAME"] = table_name
                            table["TABLE_NAME_JOIN_COL"] = table_name.split(".")[-1].strip()
                            table["LRF_SCHEMA"] = lrf_schema_file
                            table["LRF_LAYOUT"] = lrf_layout_str
                            table["TABLE_SCHEMA_MAPPINGS"] = json.dumps(column_name_mappings)
                            table["NATURAL_KEYS"] = json.dumps(natural_keys)
                            table["NEW_COLUMNS"] = json.dumps(additional_columns)
                            table["NEED_VALIDATION"] = need_validation
                            table["PARSING_ERROR"] = parsing_error
                            table["FUNCTION_MAPPINGS"] = json.dumps(function_mappings)
                            table["DELETE_WHERE_CLAUSE"] = table_name_deletes.get(table_name, "")
                            table["SELECTIVE_INSERTS"] = selective_clause
                            final_udfs_functions.extend(list(function_mappings.values()))
                            result.append(table.copy())
                        except Exception as e:
                            print(f"Having error parsing the file {filename}")
                            print(traceback.print_exc())
                            need_validation = "yes"
                            table["FILE_NAME"] = filename
                            table["TABLE_NAME"] = table_name
                            table["TABLE_NAME_JOIN_COL"] = table_name.split(".")[-1].strip()
                            table["LRF_SCHEMA"] = None
                            table["LRF_LAYOUT"] = None
                            table["TABLE_SCHEMA_MAPPINGS"] = None
                            table["NATURAL_KEYS"] = None
                            table["NEW_COLUMNS"] = None
                            table["NEED_VALIDATION"] = need_validation
                            table["FUNCTION_MAPPINGS"] = None
                            table["DELETE_WHERE_CLAUSE"] = None
                            table["SELECTIVE_INSERTS"] = None
                            parsing_error = str(e)
                            table["PARSING_ERROR"] = parsing_error
            except Exception as e:
                print(f"Having error parsing the file {filename}")
                print(str(e))
                print(traceback.print_stack())
    print(f"Writing the result to csv {cwd}/table_schema.csv ")
    resultant_df = pd.DataFrame(result)
    table_details_df = pd.read_csv(args.get("table_details_csv"))
    print("table_details_df:",table_details_df.info())
    table_details_df['Table Name'] = table_details_df['Table Name'].str.upper()
    resultant_df = pd.merge(resultant_df, table_details_df, left_on="TABLE_NAME_JOIN_COL", right_on="Table Name",
                            how="inner")
    resultant_df.drop("Table Name", axis=1, inplace=True)
    if not resultant_df.empty:
        resultant_df['Validation_Lrf_Layout'] = resultant_df.apply(lambda row: validate_lrf(row), axis=1)

    # resultant_df.drop("TABLE_NAME_JOIN_COL", axis=1, inplace=True)

    resultant_df.to_csv("./table_schema.csv", index=False)
    print("Wrote successfully!")
    #print(final_udfs_functions)


if __name__ == '__main__':
    main()
