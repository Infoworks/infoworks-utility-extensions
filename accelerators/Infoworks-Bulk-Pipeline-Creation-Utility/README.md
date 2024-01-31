# Bulk Pipeline Creation Utility

## Purpose

This utility helps in creation of Infoworks pipelines in bulk under a given domain.
It takes source id as the parameter and for each table under that source, it creates a pipeline.
This will accelerate the development activity when you simply want to create a pipeline per table under given source.

## Prerequisites
1) Domain must be created under which pipelines are to be created by Admin.
2) Infoworks Data Source of interest must be added to the above domain
3) The user whose refresh token will be used in the bulk pipeline creation must have access to create pipeline in the domain.

## Infoworks Version support:
 5.5 +

## Installation/Usage:
1) Download the package from the below link


2) Untar the package
```sh
tar -xvf bulk_pipeline_creation_utility.tar.gz
```

3) Navigate to the below directory
```sh
cd bulk_pipeline_creation_automation
```

4) Edit the configurations json file as needed.
```json
{
 "protocol": "https",
 "host": "att-iwx-pri.infoworks.technology",
 "port": "443",
 "refresh_token": "<your infoworks user refresh_token>",
 "environment_name": "azure_environment_new",
 "domain_name": "CustomTarget",
 "storage_name": "Default_Storage",
 "compute_template_name": "Default_Persistent_Compute_Template",
 "batch_engine": "spark",
 "target_schema_name": "test_schema",
 "pipeline_name_prefix": "SNOWFLAKE_TO_POSTGRES",
 "delete_older_pipeline_versions": false, 
 "exclude_audit_columns": true
}
```


<html><table class="c3"><tr class="c19"><td class="c5 c16" colspan="1" rowspan="1"><p class="c12"><span class="c15">Parameter</span></p></td><td class="c2 c16" colspan="1" rowspan="1"><p class="c12"><span class="c15">Description</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">protocol</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Protocol for making REST API calls http/https.</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">host</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Hostname of Infoworks installation.</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c12"><span class="c1">port</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Port of Infoworks RESTAPI (3000 for http/443 for https).</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c12"><span class="c1">refresh_token</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Infoworks user refresh token to make RESTAPI call.</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">environment_name</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Name of Infoworks environment under which pipelines are to be created.</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">domain_name</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Name of the Infoworks domain under which pipelines are to be created.</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">storage_name</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Name of the storage name to be associated with pipelines by default (For azure databricks environment pipelines only).</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">compute_template_name</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Name of the compute template name to be associated with pipelines by default (For azure databricks environment pipelines only).</span></p></td></tr><tr class="c10"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">batch_engine</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Name of the batch engine (spark/snowflake)</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">target_schema_name</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Name of the default target schema for pipelines target table. </span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">pipeline_name_prefix</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Pipeline prefix that must be attached to pipeline name while creating pipeline.</span></p><p class="c12 c7"><span class="c1"></span></p></td><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">exclude_audit_columns</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Whether or not to exclude Infoworks audit columns<i>(Default : true)</i></span></p><p class="c12 c7"><span class="c1"></span></p></td></tr></table></html>


5) Run the source metadata generator script
```sh
python source_tables_metadata_generator.py --source_id <source id in Infoworks> --config_json_path <full path to the configurations json file>
```

Where,

<table class="c1"><tr class="c10"><td class="c23" colspan="1" rowspan="1"><p class="c12"><span class="c18">Parameter</span></p></td><td class="c7 c28" colspan="1" rowspan="1"><p class="c12"><span class="c18">Description</span></p></td></tr><tr class="c10"><td class="c24" colspan="1" rowspan="1"><p class="c12 c17"><span class="c4">source_id</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c12"><span class="c4">Pipelines will be created for each table under this given source ID in Infoworks.</span></p></td></tr><tr class="c10"><td class="c24" colspan="1" rowspan="1"><p class="c12 c17"><span class="c4">config_json_path</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c12"><span class="c4">Absolute path to the configuration.json file (Default: ./conf/configurations.json)</span></p></td></tr><tr class="c10"><td class="c24" colspan="1" rowspan="1"><p class="c12"><span class="c4">include_table_ids</span></p><p class="c12 c17"><span class="c4">(Optional)</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c12"><span class="c4">IDs of selective tables under source,only for which pipelines should be created (Default all tables under source)</span></p></td></tr><tr class="c10"><td class="c24" colspan="1" rowspan="1"><p class="c12"><span class="c4">exclude_table_ids</span></p><p class="c12 c17"><span class="c4">(Optional)</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c12"><span class="c4">IDs of tables unders the source for which pipeline should not be created (Default: No table will be excluded)</span></p></td></tr><tr class="c10"><td class="c24" colspan="1" rowspan="1"><p class="c12 c17"><span class="c4">metadata_csv_path<br>(Optional)</span></p></td><td class="c7" colspan="1" rowspan="1"><p class="c12 c17"><span class="c4">Pass the absolute path where pipeline_metadata is to be stored along with filename.</span></p><p class="c12 c17"><span class="c4">By default: ./metadata_csv/{iwx_source_name}_pipeline_metadata.csv</span></p><p class="c12 c2"><span class="c4"></span></p></td></tr></table>

Output:

The above script generates a CSV file under metadata_csv directory.
The CSV file name will be of format <Infoworks_Source_Name>_pipeline_metadata.csv. This CSV will contain the metadata for all the pipelines it is going to create in next step.

Example:
```csv
pipeline_name,batch_engine,domain_name,environment_name,storage_name,compute_template_name,ml_engine,source_table_name,source_table_schema,catalog_name,catalog_is_database,natural_keys,sync_type,watermark_columns,target_table_name,target_table_schema
pipeline_CUSTOMERS,snowflake,sf_domain,Snowflake_env,,,SparkML,CUSTOMERS,SALESDB,SALESDB,True,"['CUSTOMERID', 'COMPANYNAME']",full-load,,CUSTOMERS,test_schema
pipeline_ORDERS,snowflake,sf_domain,Snowflake_env,,,SparkML,ORDERS,SALESDB,SALESDB,True,['ORDERID'],incremental,['ORDERID'],ORDERS,test_schema
```
6) Prepare your SQL file under sql directory.
For Example:
Create a file select_all.sql with contents as follow:
```sql

select * from {source_table_schema}.{source_table_name}

```
Where,
source_table_schema and source_table_name are column names in CSV file.
This way for every row in metadata csv, we will be dynamically generating the SQL files as per the column values.
Let us say, if the source_table_schema is *TPCH_SF1000* and source_table_name is *CUSTOMER*, then the SQL generated is as follow:
```sql
select * from TPCH_SF1000.CUSTOMER
```

**More Examples**:

If you would like to have more complex SQL then modify the metadata CSV and SQL file accordingly as shown below. 

**Example** : Let us say we would like to add derive column using an encryption function in pipeline.

I will first update the metadata CSV as follows to have two additional columns(*encryption_column*,*derive_column_name*) that changes for each pipeline
metadata csv:
```csv
pipeline_name,batch_engine,domain_name,environment_name,storage_name,compute_template_name,ml_engine,source_table_name,source_table_schema,catalog_name,catalog_is_database,natural_keys,sync_type,update_strategy,watermark_columns,target_table_name,target_table_schema,target_base_path,additional_params,source_id,table_id,encryption_column,derive_column_name
SF_TO_NORMAL_TARGET_CUSTOMER_10,spark,test_domain,CICD_environment_azure,Default_Storage,Default_Persistent_Compute_Template_2,SparkML,CUSTOMER,TPCH_SF10,SNOWFLAKE_SAMPLE_DATA,True,,full-load,,,CUSTOMER_10,public,/iw/sources/SF_TO_NORMAL_TARGET_CUSTOMER_10,,655f3dc4c8ffae4ceeb3a437,655f47f88b8e9c0007635a48,C_NAME,ENCRYPTED_C_NAME
```

And update the SQL file as follows:

```sql
select {source_table_schema}.{source_table_name}.*,encryption_function({encryption_column}) as {derive_column_name} from {source_table_schema}.{source_table_name}
```
where,
{source_table_schema},{source_table_name},{encryption_column} and {derive_column_name} will be auto resolved from csv for each row (for each pipeline). 
This way you could add any number of additional columns to csv to make a more generic SQL file that gets auto populated from CSV.

The resolved SQL that gets generated is as follow:
```sql
select TPCH_SF10.CUSTOMER.*,encryption_function(C_NAME) as ENCRYPTED_C_NAME from TPCH_SF10.CUSTOMER
```


7) Run the bulk_pipeline_creation_automation.py script to create the pipelines in bulk.

```sh
python bulk_pipeline_creation_automation.py --metadata_csv_path ./metadata_csv/rdbms_sqlserver_azure_pipeline_metadata.csv --config_json_path ./conf/configurations.json --target_node_type postgres_target --target_node_properties_file ./conf/postgres_target_properties.json --sql_file_path ./sql/s3_target_to_SF.sql
```


Where,
<table class="c3"><tr class="c10"><td class="c5 c16" colspan="1" rowspan="1"><p class="c12"><span class="c15">Parameter</span></p></td><td class="c2 c16" colspan="1" rowspan="1"><p class="c12"><span class="c15">Description</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">metadata_csv_path</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Absolute path to the metadata csv generated in step 5 including file name.</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">config_json_path</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Absolute path to the configuration json file (./conf/configurations.json)</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">target_node_type</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span>Type of target node </span><span class="c28 c23 c35">([custom_target,sql_server,oracle,postgres])</span></p></td></tr><tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">target_node_properties_file</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Absolute path to Target node properties json file including file name.If you have multiple files pass their path space separated.</span></p></td></tr>
<tr class="c19"><td class="c5" colspan="1" rowspan="1"><p class="c9"><span class="c1">sql_file_path</span></p></td><td class="c2" colspan="1" rowspan="1"><p class="c12"><span class="c1">Absolute path to SQL File.</span></p></td></tr>
</table>

### Sample target properties file:

#### The below example is for a vertica custom target:
```json
{
  "extension_execution_type": "JAVA",
  "extension_class": "com.infoworks.VerticaCustomTarget",
  "mappings": [{
        "key": "jdbc_url",
        "value": "jdbc:vertica://<hostname>:5433/<database_name>?DISABLE_DATACHECKS=true"
     },
     {
        "key": "user_name",
        "value": "<vertica_user_name>"
     },
     {
        "key": "encrypted_password",
        "value": "<encrypted_password_here"
     },
     {
        "key": "build_mode",
        "value": "overwrite"
     },
     {
        "key": "spark_write_options",
        "value": "numPartitions=2;batchsize=10000"
     },
     {
        "key": "user_managed_table",
        "value": "false"
     },
     {
        "key": "convert_columns_uppercase",
        "value": "true"
     },
     {
        "key": "natural_key",
        "value": ""
     }
  ],
  "extension_id": "6513a3e3d705990006833e0a"
}
```

#### Example template for postgres target:
```json
{
   "partition_type": "none",
    "index_type": "none",
    "data_connection_sub_type": "POSTGRES",
    "build_mode": "OVERWRITE",
    "is_existing_schema": false,
    "is_existing_table": false,
    "enable_schema_sync": false,
    "natural_key": [
     "ORDERID"
     ],
     "data_connection_id": "0265c5e4d6b8a654d60b71a5",
     "data_connection_properties": {},
     "table_name": "tets_table",
      "schema_name": "test_Schema"
 }
```
#### Example template for sql server target:
```json
{
"data_connection_sub_type": "SQL_SERVER",
"build_mode": "OVERWRITE",
"is_existing_schema": false,
"is_existing_table": true,
"enable_schema_sync": true,
"natural_key": [
    "ORDERID",
    "CUSTOMERID"
],
"data_connection_id": "655f0f322fe355069da6a366",
"data_connection_name": "SQL_SERVER_TARGET_CONNECTION",
"data_connection_properties": {},
"table_name": "test_table",
"schema_name": "test_schema",
"partition_type": "none",
"index_type": "none"
}
```
#### Example template for normal target writing to datalake:
```json
{
              "scd2_granularity": null,
              "scd1_columns": null,
              "target_schema_name": "test_schema_prd",
              "target_table_name": "test_table_prd",
              "target_base_path": "/iw/sources/abcd/test_table_prd/",
              "partition_key": [],
              "natural_key": [
                "employee_id",
                "first_name"
              ],
              "deleted_records_column": null,
              "sync_type": "OVERWRITE",
              "storage_format": "DELTA",
              "scd_type": "SCD_1",
              "sort_by_columns": []
}
```

## Output:


#### source_tables_metadata_generator.py execution:
```sh
/usr/local/bin/python3.9 ./source_tables_metadata_generator.py --source_id 64afe46e3e66f4a1b8e0c10f --config_json_path ./conf/configurations.json
Check the log file /Users/nitin.bs/PycharmProjects/infoworks-extensions/utilities/Bulk_pipeline_creation_automation/logs//pipeline_bulk_configuration.log for detailed logs
2023-10-05 12:19:32,322 — [ INFO ] - Collecting details for Table Name : CATEGORIES
2023-10-05 12:19:32,322 — [ INFO ] - Collecting details for Table Name : EMPLOYEES
2023-10-05 12:19:32,323 — [ INFO ] - Collecting details for Table Name : ORDER_DETAILS
2023-10-05 12:19:32,323 — [ INFO ] - Collecting details for Table Name : ORDERS
2023-10-05 12:19:32,323 — [ INFO ] - Collecting details for Table Name : schema_changes
2023-10-05 12:19:32,325 — [ INFO ] - Writing the metadata to /Users/nitin.bs/PycharmProjects/infoworks-extensions/utilities/Bulk_pipeline_creation_automation/metadata_csv/rdbms_sqlserver_azure_pipeline_metadata.csv
Process finished with exit code 0
```
#### bulk_pipeline_creation_automation.py execution:
```sh
/usr/local/bin/python3.9 bulk_pipeline_creation_automation.py --metadata_csv_path ./metadata_csv/rdbms_sqlserver_azure_pipeline_metadata.csv --config_json_path ./conf/configurations.json --target_node_type custom_target --target_node_properties_file ./conf/vertica_target_properties.json
Check the log file ./logs//pipeline_bulk_configuration.log for detailed logs
2023-10-05 11:51:51,433 — [ INFO ] - Creating pipeline SNOWFLAKE_TO_VERTICA_CATEGORIES under CustomTarget domain if not exists
2023-10-05 11:51:51,434 — [ INFO ] - Checking if pipeline with same name already exists
2023-10-05 11:51:51,969 — [ INFO ] - Found an existing pipeline with the same name. Using its id 65151480e6e8d27b15ec700a
2023-10-05 11:51:51,969 — [ INFO ] - select * from SALESDB.CATEGORIES
2023-10-05 11:51:53,250 — [ INFO ] - sql import success:{'error': {'error_code': None, 'error_desc': None}, 'result': {'job_id': None, 'status': 'success', 'pipeline_id': '65151480e6e8d27b15ec700a', 'response': {'message': 'Successfully imported the SQL.', 'result': {'import_response': {'processing_stage': 'PIPELINE'}, 'status': 'success'}}}}
2023-10-05 11:51:53,960 — [ INFO ] - Pipeline target properties updated successfully
2023-10-05 11:51:54,289 — [ INFO ] - Deleting any older pipeline versions other than active version
2023-10-05 11:51:54,818 — [ INFO ] - Deleting pipeline version 651e55e1c391b91951495f68
2023-10-05 11:51:55,115 — [ INFO ] - Successfully removed Pipeline Version
2023-10-05 11:51:55,115 — [ INFO ] - Deleting pipeline version 651e5601c391b91951495f72
2023-10-05 11:51:55,413 — [ INFO ] - Successfully removed Pipeline Version
2023-10-05 11:51:55,413 — [ INFO ] - Creating pipeline SNOWFLAKE_TO_VERTICA_EMPLOYEES under CustomTarget domain if not exists
2023-10-05 11:51:55,413 — [ INFO ] - Checking if pipeline with same name already exists
2023-10-05 11:51:55,930 — [ INFO ] - Found an existing pipeline with the same name. Using its id 651514bae6e8d27b15ec7015
2023-10-05 11:51:55,931 — [ INFO ] - select * from SALESDB.EMPLOYEES
2023-10-05 11:51:57,542 — [ INFO ] - sql import success:{'error': {'error_code': None, 'error_desc': None}, 'result': {'job_id': None, 'status': 'success', 'pipeline_id': '651514bae6e8d27b15ec7015', 'response': {'message': 'Successfully imported the SQL.', 'result': {'import_response': {'processing_stage': 'PIPELINE'}, 'status': 'success'}}}}
2023-10-05 11:51:58,305 — [ INFO ] - Pipeline target properties updated successfully
2023-10-05 11:51:58,642 — [ INFO ] - Deleting any older pipeline versions other than active version
2023-10-05 11:51:59,180 — [ INFO ] - Deleting pipeline version 651e55e9c391b91951495f6b
2023-10-05 11:51:59,474 — [ INFO ] - Successfully removed Pipeline Version
2023-10-05 11:51:59,474 — [ INFO ] - Deleting pipeline version 651e5605c391b91951495f73
2023-10-05 11:51:59,771 — [ INFO ] - Successfully removed Pipeline Version
2023-10-05 11:51:59,771 — [ INFO ] - Creating pipeline SNOWFLAKE_TO_VERTICA_ORDER_DETAILS under CustomTarget domain if not exists
2023-10-05 11:51:59,772 — [ INFO ] - Checking if pipeline with same name already exists
2023-10-05 11:52:00,288 — [ INFO ] - Found an existing pipeline with the same name. Using its id 651514bedc6a85020d71e39b
2023-10-05 11:52:00,289 — [ INFO ] - select * from SALESDB.ORDER_DETAILS
2023-10-05 11:52:01,784 — [ INFO ] - sql import success:{'error': {'error_code': None, 'error_desc': None}, 'result': {'job_id': None, 'status': 'success', 'pipeline_id': '651514bedc6a85020d71e39b', 'response': {'message': 'Successfully imported the SQL.', 'result': {'import_response': {'processing_stage': 'PIPELINE'}, 'status': 'success'}}}}
2023-10-05 11:52:02,505 — [ INFO ] - Pipeline target properties updated successfully
2023-10-05 11:52:02,836 — [ INFO ] - Deleting any older pipeline versions other than active version
2023-10-05 11:52:03,439 — [ INFO ] - Deleting pipeline version 651e55edb88da70f93230df9
2023-10-05 11:52:03,737 — [ INFO ] - Successfully removed Pipeline Version
2023-10-05 11:52:03,737 — [ INFO ] - Deleting pipeline version 651e5609b88da70f93230e02
2023-10-05 11:52:04,053 — [ INFO ] - Successfully removed Pipeline Version
```

## Bulk workflow creation script

This script will create one workflow per table group under given source 

### Usage:

```sh
python bulk_workflow_creation.py --source_id <source_id in iwx> --config_json_path <absolute path to configurations.json>
```
Where,
<table class="c16"><tr class="c30"><td class="c9 c25" colspan="1" rowspan="1"><p class="c24"><span class="c18 c28 c21">Parameter</span></p></td><td class="c0 c25" colspan="1" rowspan="1"><p class="c24"><span class="c18 c28 c21">Description</span></p></td></tr><tr class="c14"><td class="c9" colspan="1" rowspan="1"><p class="c13"><span class="c2">source_id</span></p></td><td class="c0" colspan="1" rowspan="1"><p class="c24"><span class="c2">Source ID under which the table groups are located</span></p></td></tr><tr class="c14"><td class="c9" colspan="1" rowspan="1"><p class="c13"><span class="c2">config_json_path</span></p></td><td class="c0" colspan="1" rowspan="1"><p class="c24"><span class="c2">Absolute path to the configuration json file (./conf/configurations.json)</span></p></td></tr></table>


## Admin Activity

### Utility to add pipeline extension to all domains:

This utility helps in attaching the given set of pipeline extensions to all the domains in bulk.

**Note**: 
Below utility can only be run using the admin user refresh token to attach the pipeline extension to all the domains in bulk.

Usage:
```sh
python add_pipeline_extension_to_all_domains.py --pipeline_extension_ids <ids of pipeline extension space separated> --config_json_path ./config.json
```
Where,

<table class="c6"><tr class="c36"><td class="c12 c32" colspan="1" rowspan="1"><p class="c14"><span class="c27 c29 c22">Parameter</span></p></td><td class="c1" colspan="1" rowspan="1"><p class="c14"><span class="c27 c29 c22">Description</span></p></td></tr><tr class="c3"><td class="c12" colspan="1" rowspan="1"><p class="c13"><span class="c5">pipeline_extension_ids</span></p></td><td class="c18" colspan="1" rowspan="1"><p class="c14"><span class="c5">IDs of pipeline extensions to be added to all domains (space separated)</span></p></td></tr><tr class="c3"><td class="c12" colspan="1" rowspan="1"><p class="c13"><span class="c5">config_json_path</span></p></td><td class="c18" colspan="1" rowspan="1"><p class="c14"><span class="c5">Absolute path to the configuration json file (./conf/configurations.json)</span></p></td></tr></table>

### Authors

Nitin BS (nitin.bs@infoworks.io)