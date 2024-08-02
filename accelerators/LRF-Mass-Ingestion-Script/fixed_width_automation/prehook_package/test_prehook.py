import argparse
import os
import subprocess

import pandas as pd

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Prehook for Snowflake sql push down of delete statement')
    parser.add_argument('--database_name', type=str, required=True, help='Pass the database name on snowflake')
    parser.add_argument('--path_to_additional_jars', type=str, required=True,
                        help='Pass the path_to_additional_jars in dbfs Eg:/FileStore/xyz.jar')
    parser.add_argument('--environment_home', type=str, required=True, help='Pass the environment_home path. Can be '
                                                                            'found in '
                                                                            '/opt/infoworks/conf/conf.properties')
    args = parser.parse_args()
    path_to_additional_jars = args.path_to_additional_jars.strip("/")
    path_to_additional_jars = path_to_additional_jars.lstrip("dbfs://")
    path_to_additional_jars = os.path.join("/", "dbfs", path_to_additional_jars, "*")
    environment_home = args.environment_home.strip("/")
    database_name = args.database_name
    job_id = os.getenv('jobId', default=None)
    table_id = os.getenv('tableId', default=None)
    job_type = os.getenv('jobType', default=None)
    try:
        file_path = os.path.dirname(__file__)
        table_delete_statements_df = pd.read_csv(f"{file_path}/table_delete_statements.csv")
        target_table_name = os.getenv('targetTableName', default=None)
        target_schema_name = os.getenv('targetSchemaName', default=None)
        where_clause = table_delete_statements_df.query(f"TABLE_NAME_JOIN_COL == '{target_table_name}'").fillna('')[
            'DELETE_WHERE_CLAUSE'].tolist()
        if where_clause:
            where_clause = where_clause[0]
        if target_table_name and target_schema_name and where_clause and job_type in ["source_crawl"]:
            print("Deleting the data from the target table if any")
            print("DELETE SQL TO RUN:")
            print(f"DELETE FROM {database_name}.{target_schema_name}.{target_table_name} WHERE {where_clause}")
            del_statement = f"DELETE FROM {database_name}.{target_schema_name}.{target_table_name} WHERE {where_clause}"
            sql_file_name = f"{file_path}/{job_id}_{table_id}.sql"
            with open(sql_file_name, "w") as f:
                f.write(del_statement)
            java_cmd_to_run = s = f"""java -Dlog4j2.configurationFile=file:log4j.properties -Xmx512m -cp {path_to_additional_jars}:/dbfs/{environment_home}/platform/_opt_infoworks_platform_bin/*:/dbfs/{environment_home}/platform/_opt_infoworks_lib_platform_common/*:/dbfs/{environment_home}/platform/_opt_infoworks_lib_platform_metadb/* io.infoworks.parser.snowflake.SnowSQLUtil -c {file_path}/user.properties -s {sql_file_name} -r prehook_sql_push"""
            print("JAVA COMMAND TO RUN:")
            print(java_cmd_to_run)
            result = subprocess.run(java_cmd_to_run.split(" "), capture_output=True, encoding='UTF-8')
            print(f"Successfully executed prehook {result}")
            print(f"Stderr:{result.stderr}")
            os.remove(sql_file_name)
        else:
            print("Did not find any delete statements")
    except (KeyError, IndexError) as e:
        print("Script execution failed {} ".format(str(e)))
