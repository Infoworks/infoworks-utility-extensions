# Utility to configure the Infoworks tables in bulk

## Table of Contents
- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Flowdiagram](#flowdiagram)
- [Installation](#installation)
- [Usage](#usage)
- [Example](#example)
- [Authors](#authors)

## Introduction:
Currently via Infoworks, individual tables need to be configured manually by going to the configure section of each table. During this process Customer should select the natural keys,ingestion type,split by column,watermark column in case of incremental ingestion,provide append/merge strategy, configure the export for each table and provide the connection details in sync to target.

## Prerequisites:
- Python3.x version installed on the machine on which the script will be run.

- Source must be created in Infoworks, tables must be added and metacrawled before running the bulk_table_configure script.

- VM from which the scripts are run should have access to teradata server to fetch the details like probable natural keys etc

- VM from which scripts are run should have access to infoworks server (on port 443 for https and port 3000 for http) to make the infoworks restapi calls.

## Flowdiagram
![RDBMS Automated table configuration flow diagram](./images/automation_demo.png?raw=true "RDBMS Automated table configuration flow diagram")

Intermediate CSV sample file structure(first few rows) from teradata source:(full CSV can be found here)

### Sub Set Filters

Syntax to be followed to configure sub set filters:

Combinational conditions i.e. AND, OR should be enclosed within in square brackets ([AND], [and], [OR], [or])

These are the conditions/operators supported currently
```
<=>, <>, <=, >=, =, <, >, is null, IS NULL, is not null, IS NOT NULL, like, LIKE, not like, NOT LIKE, rlike, RLIKE, in, IN, not in, NOT IN
```
Usage of any other operator other than these might produce unexpected results.

Examples: Actionid=value [or] actionid is null, AllocGroupId is not null


## Installation:
```bash
wget --no-check-certificate https://infoworks-releases.s3.amazonaws.com/automated_table_configuration_rdbms_55.tar.gz
```

## Usage:

Note:
Teradata/Oracle/Vertica source must be created and metacrawl must be done prior to performing below mentioned steps

- Prerequisites for teradata:
The teradata user provided should have access to the following tables:
DBC.TablesV,DBC.IndicesV,DBC.ColumnsV


1) Login to your local VM or any machine with python3 installation.
2) Download the package 
```shell
wget --no-check-certificate https://infoworks-releases.s3.amazonaws.com/automated_table_configuration_rdbms_55.tar.gz
```

Untar the package:
```shell
tar -xvf automated_table_configuration_rdbms_55.tar.gz
```

3) Navigate to untarred folder
```shell
cd ./automated_table_configuration_rdbms_55/
```

4) Create and activate a virtual env
```shell
python3 -m venv env
source ./env/bin/activate
```

5) Install any dependencies from requirements.txt
```shell
python -m pip install -r requirements.txt
```
6) Edit configurations.json file under conf directory

Configurations.json 
Location: ./conf/configurations.json
Functionality : This file is responsible for providing all the details regarding the configurations like, host name of IWX, port,environment details, ingestion format for tables etc)

( some of the commonly used updated timestamps,created timestamps that can be used as candidates for watermark columns)

If any of the keys mentioned in merge_water_marks_columns is present in current table, then that column will be used as the watermark column for the current table.

```json
{
   "protocol": "",
   "host": "",
   "port": "",
  "environment_name":"",
  "environment_storage_name":"Default_Storage",
  "environment_compute_name":"Default_Persistent_Compute_Template",
  "append_water_marks_columns":["insert_timestamp","LOADDATE","REC ADD TS","STARTDATE","CREATE_DATE"],
  "merge_water_marks_columns":["UPDATEDATE","UPDATE_DATE","CREATE_DATE","update_timestamp","REC_UPD_TS","REC CHG TS","it_update_date","OrderDate"],
  "split_by_keys":"",
  "ingestion_storage_format":"parquet",
  "exclude_audit_columns": true,
  "rtrim_string_columns": false,
  "default_table_type":"infoworks_managed_table",
  "export_configurations":{
     "sfURL": "",
     "sfAccount":"",
         "sfUser": "",
         "sfPassword": "",
         "sfDatabase": "",
         "sfSchema": "",
         "sfWarehouse": ""
  },
  "source_connection_properties": {
     "host": "",
     "port": 1025,
     "username": "",
     "encrypted_password": "",
     "default_source_db": ""
  },
  "filter_properties": {
     "is_filter_enabled":true,
     "schemas_filter": "",
     "tables_filter":""
  },
  "sfDatabase": "",
  "sfSchema": "",
  "sfStageSchema":"",
  "sfWarehouse": "DEMO_WH",
    "table_groups":[{
             "name":"tg1",
             "max_connections_to_source":4,
             "max_parallel_tables":4,
             "environment_compute_name":"Default_Persistent_Compute_Template",
             "table_names":[]

},
   {
             "name":"tg2",
             "max_connections_to_source":4,
             "max_parallel_tables":4,
             "environment_compute_name":"Default_Persistent_Compute_Template",
             "table_names":[]

}],
"run_workflow":false,
"poll_workflow":false
}
```

You could set the run_workflow to true if you want the workflow to be run after workflow creation, else false.

By default it is true.

Table names are case sensitive and must be listed as they appear in Infoworks.

More Details about individual keys of configurations.json can be found here:
https://docs.google.com/spreadsheets/d/19y2xSfc0K9-rDusVNQ0Ys3oFveyhEB21GG8n6W0puog/edit?usp=sharing


7) 
### Teradata:
```shell

python teradata_metadata_analysis_with_watermark.py --host <your_teradata_host_ip> -port <your_teradata_port> --user <your_user> --password <your_password> --schemas <space_seperated_list_of_your_selective_schemas>
```

Note: password should be encrypted using infoworks_security.sh


```shell
For K8s installation of IWX:

 The encryption script is available on the bastion host setup for AKS/when the user downloads the tar to install IWX, the script is available under the iw-k8s-installer dir:

./infoworks_security/infoworks_security.sh --encrypt -p "<password>

```



Result of above command should be encrypted password fFCuy3JaHWk5Qy7o4o4oewqtCh0RdIVBzEqELaxJ0yKJAfHAK2K05JZ9/ZUHISGgQIvPKQ==

Eg:
```shell

python teradata_metadata_analysis_with_watermark.py --host <IP> --port 1025 --user dbc --password fFCuy3JaHWk5Qy7ko4o4oewqtCh0RdIVBzEqELaxJ0yKJAfHAK2K05JZ9/ZUHISGgQIvP --schemas automation_db
```



### Snowflake:
```shell

python snowflake_metadata_analysis_with_watermark.py --account_name <your_snowflake_account_name> --user <your_user> --password <your_password> --warehouse <your_snowflake_warehouse> --database <your_snowflake_database> --schemas <space_seperated_list_of_your_selective_schemas> --tables <space_seperated_list_of_your_selective_tables> --skip_views <True/False>
```

Note: password should be encrypted using infoworks_security.sh


```shell
For K8s installation of IWX:

 The encryption script is available on the bastion host setup for AKS/when the user downloads the tar to install IWX, the script is available under the iw-k8s-installer dir:

./infoworks_security/infoworks_security.sh --encrypt -p "<password>

```



Result of above command should be encrypted password fFCuy3JaHWk5Qy7o4o4oewqtCh0RdIVBzEqELaxJ0yKJAfHAK2K05JZ9/ZUHISGgQIvPKQ==

Eg:
```shell

python snowflake_metadata_analysis_with_watermark.py --account_name iwx_account --user iwx_user --password nHsjRYuXZ+Am42KbslIH34DNUYsadjhbsjaNHudgG+KqQ== --warehouse DEMO_WH --database DATABASE_01 --schemas SOURCE_01_SCHEMA PUBLIC --skip_views True
```

### SQL Server:
```shell

python sqlserver_metadata_analysis_with_watermark.py --host <your_sqlserver_host_name> --user <your_user> --password <your_password> --database <your_sqlserver_database> --schemas <space_seperated_list_of_your_selective_schemas> --tables <space_seperated_list_of_your_selective_tables>
```

Note: password should be encrypted using infoworks_security.sh


```shell
For K8s installation of IWX:

 The encryption script is available on the bastion host setup for AKS/when the user downloads the tar to install IWX, the script is available under the iw-k8s-installer dir:

./infoworks_security/infoworks_security.sh --encrypt -p "<password>

```



Result of above command should be encrypted password fFCuy3JaHWk5Qy7o4o4oewqtCh0RdIVBzEqELaxJ0yKJAfHAK2K05JZ9/ZUHISGgQIvPKQ==

Eg:
```shell
python sqlserver_metadata_analysis_with_watermark.py --host iwx_host --user iwx_user --password nHsjRYuXZ+Am42KbslIH34DNUYsadjhbsjaNHudgG+KqQ== --database SALESDB --schemas dbo --tables Orders Persons suppliers
```




### Oracle:
Download the oracle basic clients from:
https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html

```shell

export ORACLE_HOME='path where oracle clients are downloaded and unzipped'(Eg:/home/infoworks/instantclient_21_3)

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME


python oracle_metadata_analysis_with_watermark.py --host <your_oracle_host_ip> --port <your_oracle_host_port> --service <service_id> --user <your_user> --password <your_password> --schemas <space_seperated_list_of_your_selective_schemas>

```

### Vertica:
```shell
python vertica_metadata_analysis_with_watermark.py --host <your_vertica_host_ip> --port <your_vertica_host_port> --user <your_user> --password <your_password> --schemas <space_seperated_list_of_your_selective_schemas>
```

7) Edit conf/configurations.json  according to your need

Note:Ensure to mask any of your passwords before providing in configurations json
Steps to mask(encrypt) password:
Note: password should be encrypted using infoworks_security.sh

```shell
For K8s installation of IWX:

 The encryption script is available on the bastion host setup for AKS/when the user downloads the tar to install IWX, the script is available under the iw-k8s-installer dir:
./infoworks_security/infoworks_security.sh --encrypt -p "<password>
```



Provide the output of above command in configurations json as password in export_configuration section (only for non datawarehouse environments).

8) Run the bulk table configure script
```shell
python bulk_table_configure_with_watermark.py --source_id <teradata_source_id/oracle_source_id/vertica_source_id> --refresh_token <your_refresh_token_with_admin_privileges> --domain_name <domain_where_workflows_will_be_created> --source_type <oracle/teradata/vertica> --configure <all(default)/tg/workflow>
```





Workflow with ingest node followed by sync to target node with the table groups gets configured within ingest node and export node.




## Script output:

```shell

sourcsource_typee_source_typesource_typetype
DatabaseName :  SALES_DW
TableName :  DimChannel
Columns :  ['CHANNELKEY', 'CHANNELLABEL', 'CHANNELNAME', 'CHANNELDESCRIPTION', 'ETLLOADID', 'LOADDATE', 'UPDATEDATE', 'ziw_target_timestamp', 'ziw_is_deleted']
probable_natural_keys found are  ['CHANNELKEY']
Adding split by key as month derived from  LOADDATE
Configuring table for incremental merge mode
Setting Merge Mode with watermark Column :  UPDATEDATE





Configuring the table export with below configs
{'sfURL': '<your_sfdc_account_url>', 'sfUser': '<your_sfdc_user>', 'sfPassword':<your_encrypted_password>, 'sfDatabase': '<your_sfdc_database>', 'sfSchema': '<your_SFDC_schema>', 'sfWarehouse': '<your_sfdc_warehouse>'}

Configuring table for incremental export
natural Keys:  ['CHANNELKEY']
sync_type:  merge
watermark_column :  UPDATEDATE


Updated the table eb37933f5ac0f5b3efc90ff5 configurations successfully



Configuring table group with below configurations
{'environment_compute_template': {'environment_compute_template_id': '1b9e52df1a8e5c8f5fa8c9cc'}, 'name': 'tg_with_2tables', 'max_connections': 1, 'max_parallel_entities': 2, 'tables': [{'table_id': '1cdd52aa8f118ecae90a91f8', 'connection_quota': 50}, {'table_id': 'eb37933f5ac0f5b3efc90ff5', 'connection_quota': 50}]}

Configured table group tg_with_2tables successfully!
```

## More details
https://docs.google.com/document/d/17Kt6VSYtEwltP22Uit4zIOKLb6h5xQv7I4cStiqGd_A/edit?usp=sharing



## Authors

- nitin.bs@infoworks.io
- sanath.singavarapu@infoworks.io



