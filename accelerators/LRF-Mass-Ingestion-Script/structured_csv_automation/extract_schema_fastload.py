import json
import os
import re
from typing import List
import csv
import pandas as pd
from antlr4 import *
from colorama import Fore, Style

from parser.TeradataLexer import TeradataLexer
from parser.TeradataListener import TeradataListener
from parser.TeradataParser import TeradataParser
import argparse
cwd=os.getcwd()
configuration_file= open(f"{cwd}/conf/configurations.json","r")
configuration_json=json.load(configuration_file)
class TPTExpressionExtractor(TeradataListener):

    def __init__(self, stream: CommonTokenStream):
        self.column_expr = {}
        self.tokens: CommonTokenStream = stream

    def getColumnExpr(self):
        return self.column_expr

    def exitInsertCallback(self, ctx: TeradataParser.InsertCallbackContext):
        insertIntoContext: TeradataParser.InsertIntoContext = ctx.insertInto()
        identifierList: TeradataParser.IdentifierListContext = insertIntoContext.identifierList()
        valuesExpression: TeradataParser.InlineTableContext = insertIntoContext.valuesExpression()
        if identifierList is not None and valuesExpression is not None:
            seq: TeradataParser.IdentifierSeqContext = identifierList.identifierSeq()
            column_identifiers: List[TeradataParser.IdentifierContext] = seq.identifier()

            for index, column_identifier in enumerate(column_identifiers):
                column_identifier: TeradataParser.IdentifierContext = column_identifier
                values_expression: TeradataParser.ExpressionContext = valuesExpression.expression(i=index)
                self.column_expr[column_identifier.getText()] = \
                    self.tokens.getText(values_expression.start, values_expression.stop)


def extract_tpt_columns(sql):
    try:
        print(f"Parsing SQL: {sql}")
        data = InputStream(sql)
        lexer = TeradataLexer(data)
        stream = CommonTokenStream(lexer)
        parser = TeradataParser(stream)
        tree = parser.statement()
        extractor = TPTExpressionExtractor(stream)
        walker = ParseTreeWalker()
        # visitor = TPTColumnExtractor()
        # visitor.visit(tree)

        walker.walk(extractor, tree)
        return extractor.getColumnExpr(),""
    except Exception as e:
        return {},str(e)


def find_between(s,start,end):
    try:
        result=(s.split(start))[1].split(end)[0]
        return result
    except IndexError as e:
        print(f"Could not find the statement that starts with {start} and ends with {end}")
        print(str(e))
        return ""

def print_success(message):
    print(Fore.GREEN + message)
    print(Style.RESET_ALL)

def print_warn(message):
    print(Fore.YELLOW + message)
    print(Style.RESET_ALL)
def print_error(message):
    print(Fore.RED + message)
    print(Style.RESET_ALL)

td_spark_regex = {}
with open("conf/teradata_to_spark_conversion_regex_mapping.csv", "r") as file:
    csv_file = csv.DictReader(file)
    for row in csv_file:
        td_spark_regex[row["td_regex_pattern"]] = row["spark_mapping"]

def remove_null_values_columns(insert_sql,filename):
    columns = find_between(insert_sql, f"INSERT INTO ", "VALUES").strip()
    columns = re.sub(".*?\(","",columns)
    columns=columns[1:-1].split(",")
    columns=list(map(lambda x:x.strip(),columns))
    values = find_between(insert_sql, "VALUES", ");").strip()
    values = values[1:]
    values=re.split(r',\s*(?![^()]*\))', values)
    values = list(map(lambda x: x.strip(),values))
    final_list=list(zip(columns, values))
    final_list = list(filter(lambda x: x[1] != 'NULL', final_list))
    final_sql = f"INSERT INTO {filename} ({','.join([x[0] for x in final_list])})  VALUES ({','.join([x[1] for x in final_list])});"
    return final_sql

def add_function_mappings(insert_sql):
    #if insert_sql.lower().startswith("case when"):
    #    insert_sql=re.sub("casewhentrim\(:[a-z_]+\)=thennullelsecast\(substr\(:[a-z_]+,\d+,\d+\)\|\|-\|\|substr\(:[a-z_]+,\d+,\d+\)\|\|-\|\|substr\(:[a-z_]+,\d+,\d+\)\|\|substr\(:[a-z_]+,\d+,\d+\)astimestamp\(\d+\).*end","CustomStringToTimestampHiveUDF",insert_sql.replace("'","").replace(" ","").lower())
    #    return insert_sql
    # Find and replace equals and not equals in CASE WHEN statements
    for key in td_spark_regex:
        pattern = key
        replace_value = td_spark_regex[key]
        insert_sql = re.sub(pattern, replace_value, insert_sql).strip()
    if insert_sql.strip().lower().startswith("lit"):
        return insert_sql
    insert_sql=re.sub("(DECIMAL\(\d+,\d+\))","",insert_sql)
    insert_sql = re.sub("\(AS CHAR\(\d+\)\)", "", insert_sql)
    col_name = re.search(":.+?(?=(,|\s|\)|/))", insert_sql + ",")
    if not col_name:
        return f"lit({insert_sql})"
    else:
        col_name = col_name.group().strip().strip(":")
    if bool(re.search("(AS TIMESTAMP\(\d\) FORMAT| as timestamp\(\d\) format)",insert_sql)):
        if bool(re.search("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",insert_sql)):
            insert_sql=re.sub("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",f":{col_name}",insert_sql)
        if bool(re.search("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",insert_sql)):
            insert_sql=re.sub("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",f":{col_name}",insert_sql)
        insert_sql = re.sub("(AS TIMESTAMP\(\d\) FORMAT| as timestamp\(\d\) format)",",",insert_sql)
        insert_sql=re.sub("(CAST|cast|Cast)","to_timestamp",insert_sql)
        if insert_sql.startswith("("):
            insert_sql = insert_sql.replace("(", "to_timestamp(", 1)
    if bool(re.search("(AS TIMESTAMP\(\d\)\)|as timestamp\(\d\)\))",insert_sql)):
        if bool(re.search("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",insert_sql)):
            insert_sql=re.sub("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",f":{col_name}",insert_sql)
        if bool(re.search("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",insert_sql)):
            insert_sql=re.sub("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",f":{col_name}",insert_sql)
        insert_sql = re.sub("(AS TIMESTAMP\(\d\)\)|as timestamp\(\d\)\))", "",insert_sql)
        insert_sql=re.sub("(CAST|cast|Cast)","",insert_sql)
        if insert_sql.startswith("("):
            insert_sql = insert_sql.replace("(", "to_timestamp(", 1)
    if bool(re.search("(AS DATE FORMAT|as date format|as date FORMAT|as DATE FORMAT)",insert_sql)):
        if bool(re.search("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",insert_sql)):
            insert_sql=re.sub("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",f":{col_name}",insert_sql)
        if bool(re.search("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",insert_sql)):
            insert_sql=re.sub("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",f":{col_name}",insert_sql)
        insert_sql = re.sub("(AS DATE FORMAT|as date format|as date FORMAT|as DATE FORMAT)", ",",insert_sql)
        insert_sql=re.sub("(CAST|cast|Cast)","to_date",insert_sql)
        if insert_sql.startswith("("):
            insert_sql=insert_sql.replace("(","to_date(",1)
    if bool(re.search("(AS TIME FORMAT|as time format|as time FORMAT|as TIME FORMAT)",insert_sql)):
        if bool(re.search("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",insert_sql)):
            insert_sql=re.sub("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",f":{col_name}",insert_sql)
        if bool(re.search("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",insert_sql)):
            insert_sql=re.sub("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",f":{col_name}",insert_sql)
        insert_sql = re.sub("(AS TIME FORMAT|as time format|as time FORMAT|as TIME FORMAT)", ",",insert_sql)
        insert_sql=re.sub("(CAST|cast|Cast)","to_date",insert_sql)
    if bool(re.search("(AS TIME\(\d\)|as time\(\d\))",insert_sql)):
        if bool(re.search("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",insert_sql)):
            insert_sql=re.sub("((trim|TRIM)\s*\([:a-zA-Z0-9_]+\))",f":{col_name}",insert_sql)
        if bool(re.search("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",insert_sql)):
            insert_sql=re.sub("((substr|SUBSTR)\s*\([:a-zA-Z0-9_]+,\d+,\d+\))",f":{col_name}",insert_sql)
        insert_sql = re.sub("( AS TIME\(\d\) FORMAT)", f",",insert_sql)
        insert_sql = re.sub("(AS TIME\(\d\))", f",hh:mi:ss", insert_sql)
        insert_sql=re.sub("(CAST|cast|Cast)","to_timestamp",insert_sql)
        if insert_sql.startswith("("):
            insert_sql=insert_sql.replace("(","to_timestamp(",1)
    insert_sql = insert_sql.replace("cast(", "").replace("''","\\'")
    insert_sql = insert_sql.replace("CAST(", "").replace("''", "\\'")
    if bool(re.search("(FORMAT|format|Format)", insert_sql)):
        insert_sql = re.sub("(FORMAT|format|Format)", ",", insert_sql)
    insert_sql = insert_sql.replace("()","")
    if insert_sql.lower().replace(" ","").startswith("casewhen"):
        for item in re.findall(
                "((?:WHEN\s*[:(\w]+)\s*(?:IS|is|Is|NE|ne|Ne|EQ|eq|Eq|=|<>|!=)?)(\s*'?[\w\s?_@.#&+-]*'?\s*\)?\s*(?:THEN|then|Then))",
                insert_sql):
            i, j = item
            if i.strip().upper().endswith("NE"):
                actual_string = i + j
                string_to_replace = i.rstrip('NE').rstrip('ne').rstrip('Ne') + " <> " + j
                insert_sql = insert_sql.replace(actual_string, string_to_replace)
            elif i.strip().upper().endswith("EQ"):
                actual_string = i + j
                string_to_replace = i.rstrip('EQ').rstrip('eq').rstrip('Eq') + " = " + j
                insert_sql = insert_sql.replace(actual_string, string_to_replace)
            elif i.endswith("=") or i.endswith("!=") or i.endswith("<>") or i.upper().endswith("IS"):
                pass
            else:
                actual_string = i + j
                string_to_replace = i + " = " + j
                insert_sql = insert_sql.replace(actual_string, string_to_replace)
    return insert_sql

def removeComments(string):
    string = re.sub(re.compile("/\*.*?\*/",re.DOTALL ) ,"" ,string) # remove all occurrences streamed comments (/*COMMENT */) from string
    string = re.sub(re.compile("//.*?\n" ) ,"" ,string) # remove all occurrence single-line comments (//COMMENT\n ) from string
    return string

def main():
    parser = argparse.ArgumentParser('Extract schema from Teradata loader scripts')
    parser.add_argument('--path_to_ctl_files', required=True, help='Pass the absolute path to the directory containing Fastload ctl files')
    parser.add_argument('--table_details_csv', required=True, help='Pass the absolute path to the table details csv file containing lrf path')
    args = vars(parser.parse_args())
    folder_path=args.get("path_to_ctl_files")
    table_details_csv_path=args.get("table_details_csv")
    if not os.path.exists(folder_path):
        print("Please enter a valid path to the folder containing the ctl files")
        exit(-100)
    if not os.path.exists(table_details_csv_path):
        print("Please enter a valid path to the csv file containing the lrf path details")
        exit(-100)
    result=[]
    files_in_current_directory=os.listdir(folder_path)
    table_details_df=pd.read_csv(table_details_csv_path)
    table_details_df['Table Name']=table_details_df['Table Name'].str.lower()
    file_properties = configuration_json["file_properties"]
    for filename in files_in_current_directory:
        table={}
        if filename.endswith("tpt.ctl"): #and "ccpm_wrd_sls_ord_hdr_prep" in filename:
            print_warn(f"Parsing {filename}")
            s=''
            filepath=os.path.join(folder_path,filename)
            with open(filepath,"r") as f:
                s=f.read()
                s=re.sub("\n","",s)
                s=re.sub("\s+",' ',s)
            #print(s)
            filename=filepath.split("/")[-1].split(".")[0]
            filename=filename.split(".")[0]
            s=s.replace("\n","").replace("\r",'')
            schema_res = find_between(s,"DEFINE SCHEMA",");")
            #print("schema_res:",schema_res)
            schema_res = removeComments(schema_res)
            schema_res = schema_res.replace("\n","")
            layout_name=re.search("[a-zA-Z_]+\s*?\(",schema_res).group()
            layout_name=layout_name.replace("(","\(")
            #print("layout_name:", layout_name)
            #print(schema_res)
            schema_res = re.sub(layout_name,"",schema_res)
            print("schema_str before:",schema_res)
            #schema_res = schema_res.strip().strip("(").strip(")")
            #schema_str=schema_res.strip("(").strip(")").strip()
            schema_str=schema_res.strip()
            print("schema_str after:",schema_str)
            columns = [i.strip().split(" ")[0] for i in schema_str.split(",")]
            insert_sql=find_between(s,f"INSERT INTO ",");").strip()
            if not insert_sql:
                insert_sql = find_between(s, f"insert into ", ");").strip()
            if not insert_sql:
                insert_sql = find_between(s, f"Insert into ", ");").strip()
            insert_sql = removeComments(insert_sql)
            insert_sql = f"INSERT INTO " + insert_sql + ");"

            #if insert_sql:
            #    insert_sql=remove_null_values_columns(insert_sql, filename)
            insert_sql=insert_sql.replace("''","\'")
            column_value_mappings,error=extract_tpt_columns(insert_sql)
            parsing_error=""
            need_validation='No'
            if error != "":
                parsing_error=error
                need_validation = 'Yes'
            new_columns=list(column_value_mappings.keys())
            old_columns = list(column_value_mappings.values())
            #add_load_dt="No,"
            #add_updt_dt="No,"
            natural_keys=[]
            update_statement=re.search("UPDATE.*WHERE.*?,",s)
            if update_statement:
                update_statement = update_statement.group()
                update_statement = removeComments(update_statement)
                print("update_statement",update_statement)
                where_clause = re.search("WHERE.*?,",update_statement+",")
                if where_clause:
                    where_clause=where_clause.group()
                    where_clause=where_clause.replace("WHERE","").replace(" ","").strip(",").replace("'","").replace("\"","")
                    combined_natural_keys=where_clause.split("AND")
                    for combined_natural_key in combined_natural_keys:
                        natural_keys.append(combined_natural_key.split("=")[0])
            for k,v in column_value_mappings.items():
                column_value_mappings[k]=add_function_mappings(v)
            function_mappings={}
            print(column_value_mappings)
            column_mappings={}
            print("column_value_mappings:",column_value_mappings)
            new_columns=[]
            lrf_col_names_visited = []
            columns_added_as_new_columns = []
            for k,v in column_value_mappings.items():
                v = re.sub(f"{k}\s*=\s*", " ",v)
                lrf_col_name = ""
                lrf_column_details_search = v + " "
                temp_search_result = []
                while True:
                    match = re.search(":[a-zA-Z0-9_]+?(?=(,|\s|\)|/|\|))", lrf_column_details_search)
                    if match:
                        temp_search_result.append(match.group(0))
                        lrf_column_details_search = lrf_column_details_search[match.end():]
                    else:
                        break
                temp_search_result = list(set(temp_search_result))
                if len(temp_search_result) > 0:
                    lrf_col_name = "".join(temp_search_result)
                else:
                    lrf_col_name = ""
                lrf_column_details = add_function_mappings(v.strip())
                if lrf_col_name != "":
                    lrf_col_name=lrf_col_name.strip(":")

                # lrf_name = re.search(":.+?(?=(,|\s|\)))", lrf_column_details + ",")
                if lrf_col_name is not None and len(temp_search_result) == 1:
                    if lrf_col_name in lrf_col_names_visited:
                        temp_str = f'nvl({lrf_column_details.strip().replace(f"{lrf_col_name}", lrf_col_name.strip(":"))}$$)'
                        new_columns.append({k: temp_str})
                        columns_added_as_new_columns.append(k)
                    else:
                        column_value_mappings[k] = lrf_col_name.strip(":")
                        column_mappings[lrf_col_name] = k
                        lrf_col_names_visited.append(lrf_col_name)
                else:
                    #col_name = col_name.group().strip().strip(":")
                    if lrf_column_details.strip().lower() in ["current_date",
                                                                              "current_time"] or lrf_column_details.strip().lower().startswith('lit'):
                                        new_columns.append({k: re.sub(":(?=[a-zA-Z_\d])", "", lrf_column_details.strip())})
                                        columns_added_as_new_columns.append(k)
                    else:
                        temp_replaced = re.sub(":(?=[a-zA-Z_\d])", "", lrf_column_details.strip())
                        new_columns.append({k: f"nvl({temp_replaced}$$)"})
                        columns_added_as_new_columns.append(k)

                if lrf_col_name is not None and not lrf_column_details.strip().strip(":") == lrf_col_name.strip():
                    try:
                        col_name_to_remove = re.search(":[a-zA-Z0-9_]+?(?=(,|\s|\)|/|\|))",
                                                       lrf_column_details + " ").group()
                    except:
                        col_name_to_remove = ""

                    lrf_column_details_replaced_with_empty = re.sub(f":{k}\s*,?", "",
                                                                    lrf_column_details.strip())
                    lrf_column_details_replaced_with_empty = re.sub(f"{col_name_to_remove}\s*,?", "",
                                                                    lrf_column_details_replaced_with_empty)
                    lrf_column_details_removed_colon = lrf_column_details.strip().replace(
                        f":{v}", f"{v.strip(':')}")
                    flag_replaced=False
                    if k not in columns_added_as_new_columns:
                        if flag_replaced:
                            function_mappings[k] = lrf_column_details_replaced_with_empty
                        else:
                            if "nvl2" in lrf_column_details_removed_colon:
                                function_mappings[k] = lrf_column_details_removed_colon
                            elif "to_timestamp" not in lrf_column_details.strip().lower() and "to_date" not in lrf_column_details.strip().lower() and "to_int" not in lrf_column_details.strip().lower() and "lit" not in lrf_column_details.strip().lower():
                                function_mappings[
                                    k] = f"nvl2({lrf_column_details_removed_colon},NULL)"
                            else:
                                function_mappings[k] = lrf_column_details_replaced_with_empty
                #col_name=column_mappings[col_name]
                if not v.startswith(":"):
                    if v.lower().replace(" ","").startswith("to_date"):
                        v=re.sub("(TRIM\([a-zA-Z\:0-9_]+\)|trim\([a-zA-Z\:0-9_]+\))","",v)
                        v = re.sub("(SUBSTR\([a-zA-Z\:0-9_]+\)|substr\([a-zA-Z\:0-9_]+\))", "", v)
                        format = v.split(",")[-1]
                        function_mappings[lrf_col_name] = f"to_date({format})"
                    elif v.lower().replace(" ","").startswith("to_timestamp"):
                        v=re.sub("(TRIM\([a-zA-Z\:0-9_]\)|trim\([a-zA-Z\:0-9_]\))","",v)
                        v = re.sub("(SUBSTR\([a-zA-Z\:0-9_ ]+,\s*\d+\s*,\s*\d+\s*\)|substr\([a-zA-Z\:0-9_ ]+,\s*\d+\s*,\s*\d+\s*\))", "", v)
                        timestamp_nested_count=v.count("to_timestamp")
                        format = v.split(",")[-timestamp_nested_count]
                        if format.startswith('to_timestamp'):
                            format=""
                        function_mappings[lrf_col_name] = f"to_timestamp({format})"
                    elif v.lower().strip().startswith(":"):
                        function_mappings[lrf_col_name] = v
                    # elif "casewhen" in v.lower().replace(" ",""):
                    #     function_mappings[col_name] = v
                    #     need_validation='Yes'
                    else:
                        #v = v.replace(f":{lrf_col_name}",f"{lrf_col_name}")
                        #function_mappings[lrf_col_name] = f"nvl2({v},NULL)"
                        pass
                        #need_validation = 'Yes'
                else:
                    pass

            for k,v in function_mappings.items():
            #    v = re.sub(f"{k}\s*=\s*", "",v)
                function_mappings[k]=v.replace("((","(").replace("))",")").replace("'","\'").replace("\"","")

            print("function_mappings:",function_mappings)

            print("\n")
            print_warn("LRF Schema: ")
            print(columns)
            print_warn("Target Columns: ")
            print(new_columns)
            print_warn("Column Rename Mappings: ")
            print(column_mappings)
            print_warn("Column Function Mappings: ")
            print(function_mappings)
            print_success(f"Done parsing {filename}")
            lrf_schema=','.join(columns)
            mappings=json.dumps(column_mappings)
            table["TABLE_NAME"]=filename
            table["LRF_SCHEMA"]=lrf_schema
            table["TABLE_SCHEMA_MAPPINGS"]=mappings
            table["NEED_VALIDATION"]=need_validation
            table["PARSING_ERROR"]=parsing_error
            table["NEW_COLUMNS"]=json.dumps(new_columns)
            table["FUNCTION_MAPPINGS"]=json.dumps(function_mappings)
            table["FILE_PROPERTIES"]=json.dumps(file_properties)
            table["NATURAL_KEYS"]=json.dumps(natural_keys)
            result.append(table)
        else:
            pass
    print(f"Writing the result to csv {cwd}/table_schema.csv ")
    resultant_df = pd.DataFrame(result)
    resultant_df=pd.merge(resultant_df, table_details_df,left_on="TABLE_NAME", right_on="Table Name", how="inner")
    resultant_df.drop("Table Name",axis=1,inplace=True)
    resultant_df.to_csv("./table_schema.csv", index=False)
    print("Wrote successfully!")

if __name__ == '__main__':
    main()