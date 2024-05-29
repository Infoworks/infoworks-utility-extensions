# Infoworks Metadata Extractor

## Introduction
This Product Recipe will enable clients to integrate the Software with systems that track metadata. The Product Recipe
retrieves static and dynamic information of the Infoworks objects via Infoworks APIs.


## Table of Contents
- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Acquired Attributes](#acquired-attributes)
- [Output](#output)
- [Authors](#authors)


## Prerequisites
* Python 3.6
* Infoworks 5.3.x (not compatible with earlier version of Infoworks)

## Usage

### table_metadata_extractor_V4.py
```
python3 table_metadata_extractor_V4.py --config_file <Fully qualified path of the configuration file> --output_dir <Fully qualified output directory path>
```
#### Fields inside configuration file


| **Parameter**    | **Description**                       |
|:-----------------|:--------------------------------------|
| `host`*          | IP Address of Infoworks VM            |
| `port`*          | 443 for https / 3000 for http         |
| `protocol`*      | http/https                            |
 | `refresh_token`* | Refresh Token from Infoworks Account  |

**All Parameters marked with * are Required**

### pipeline_metadata_extractor.py
```
python3 pipeline_metadata_extractor.py --config_file <Fully qualified path of the configuration file> --domains <comma separated domain names> --pipelines <comma seperated pipelines ids> --should_fetch_column_lineage <True/False>
```
#### Input Arguments


| **Parameter**                 | **Description**                                                                                                       |
|:------------------------------|:----------------------------------------------------------------------------------------------------------------------|
| `domain_names`                     | Pass the comma seperated list of domain names from which the pipeline metadata has to be fetched. |
| `domain_ids`                     | Pass the comma seperated list of domain ids from which the pipeline metadata has to be fetched |
| `pipelines`                   | Pass the comma seperated list of pipeline ids for which the pipeline metadata has to be fetched. Not mandatory field  |
| `should_fetch_column_lineage` | This flag enables/disables capturing of column level lineage for pipelines. By default it is false. Pass true/false   |

Either domain_names or domain_ids should be specified. 

#### Fields inside configuration file

| **Parameter**    | **Description**                       |
|:-----------------|:--------------------------------------|
| `host`*          | IP Address of Infoworks VM            |
| `port`*          | 443 for https / 3000 for http         |
| `protocol`*      | http/https                            |
 | `refresh_token`* | Refresh Token from Infoworks Account  |

**All Parameters marked with * are Required**

### job_metrics_extractor.py
```
python3 job_metrics_extractor.py --config_file <Fully qualified path of the configuration file> --time_range_for_jobs_in_mins < TIME_IN_MINS> --output_file <Output File Name>
```
#### Input Arguments

| **Parameter**                  | **Description**                                                       |
|:-------------------------------|:----------------------------------------------------------------------|
 | `time_range_for_jobs_in_mins`* | Time Range (in Minutes) to extract all the executed jobs in Infoworks |  
 | `output_file`| Output File Name |

#### Fields inside configuration file

| **Parameter**    | **Description**                       |
|:-----------------|:--------------------------------------|
| `host`*          | IP Address of Infoworks VM            |
| `port`*          | 443 for https / 3000 for http         |
| `protocol`*      | http/https                            |
 | `refresh_token`* | Refresh Token from Infoworks Account  |
 
**All Parameters marked with * are Required**

## Acquired Attributes
The output files contains organized and processed gathered attributes, 
which are fields of data collected from Infoworks APIs.
The attributes offer a comprehensive view of the objects created in Infoworks environment.

### Attributes from table_metadata_extractor_V4.py
| **Attribute**                | **Description**                                          |
|:-----------------------------|:---------------------------------------------------------|
| source_name                  | Name of the Infoworks Source                             |
| source_type                  | Category of Infoworks Source Connector                   |
| source_sub_type              | Connector Type of the Infoworks Source                   |
| source_connection_url        | JDBC Connection URL for RDBMS Sources                    |
 | snowflake_warehouse          | Warehouse Name of the Snowflake Source                   |
 | source_connection_username   | Username configured in Source Connection                 |
| source_schema_name           | Name of the schema at Source                             |
 | source_table_name            | Name of the table at Source                              |
| source_file_name             | Pattern to ingest files if Source Connector is File type |
| source_columns_and_datatypes | Columns and Datatypes of Table at Source                 |
| data_environment_name         | Data Environment Name attached to the Source             |
| data_environment_type         | Type of Data Environment (Azure, Snowflake etc)          |
| snowflake_profile             | Configured Profile of Snowflake Data Environment         |
| snowflake_profile_username    | Username of Snowflake Profile                            |
 | target_type                  | Connector Type of the Target configured                  |
 | target_database_name         | Name of the database at Target                           |
 | target_schema_name           | Name of the schema at Target                             |
 | target_table_name            | Name of the table at Target                              |
 | description                  | Description of the table at Infoworks                    |
 | target_columns_and_datatypes | Columns and Datatypes of Table at Infoworks              |
 | tags                         | Tags of the table at Infoworks                           |


### Attributes from job_metrics_extractor.py
| **Attribute**            | **Description**                                                                     |
|:-------------------------|:------------------------------------------------------------------------------------|
| workflow_id              | Workflow ID that triggered the Job                                                  |
| workflow_run_id          | Run Id of the workflow that triggered the Job                                       |
| job_id                   | Infoworks Job ID                                                                    |
 | entity_type              | Type of Infoworks Entity [source,table,pipeline]                                    |
 | job_type                 | Type of Infoworks Job [FULL_LOAD,INCREMENTAL, PIPELINE_BUILD,EXPORT]                |
| job_start_time           | Time when Infoworks job started                                                     |
 | job_end_time             | Time when Infoworks job ended                                                       |
 | cluster_id               | Databaricks Compute Cluster ID                                                      |
 | job_status               | Infoworks job status [RUNNING,FAILED,SUCCEEDED]                                     |
| source_name              | Name of Infoworks Source for Ingestion Jobs                                         |
 | source_file_names        | Comma Separated List of files ingested during ingestion jobs for file based sources |
| source_schema_name       | Name of schema at source                                                            |
| source_database_name     | Name of database at source                                                          |
 | table_group_name         | Infoworks Table Group name that a job belongs to                                    |
 | iwx_table_name           | Infoworks Table Name                                                                |
 | starting_watermark_value | Starting watermark value for incremental Ingestion                                  |
 | ending_watermark_value   | Ending watermark value for incremental Ingestion                                    |
 | target_schema_name       | Target table schema name(project_id.dataset name in case of Bigquery tables)        |
 | target_table_name        | Target table name for sources,pipeline and export tables                            |
 | pre_target_count         | Count of the records before job run                                                 |
 | fetch_records_count      | Count of records fetched during the job                                             |
 | target_records_count     | Final count of records in the target table                                          |
 | job_link                 | Direct link to Infoworks Job                                                        |
 | job_created_by           | User ID who triggered the job                                                       |
 | name                     | Name of the user ID                                                                 |
 | email                    | Email ID of the user                                                                |
 

### Attributes from pipeline_metadata_extractor.py
| **Attribute**                | **Description**                                               |
|:-----------------------------|:--------------------------------------------------------------|
| domain_name                  | Name of the Domain in which Pipeline is created               |
| pipeline_name                | Name of the pipeline                                          |
| number_of_versions           | Number of versions in the pipeline                            |
| active_version               | Active version Id of the pipeline                             |
 | batch_engine                 | Compute engine in which pipeline is run                       |
 | created_at                   | Time at which the pipeline was created                        |
| modified_at                  | Time at which the pipeline was modified                       |
| description                  | Description of the pipeline in Infoworks                      |
 | environment_name             | Environment in which the pipeline is created                  |
 | storage_name                 | Name of the Storage used by the pipeline                      |
 | compute_name                 | Name of the Compute used by the pipeline                      |
 | tags                         | Tags of the pipeline at Infoworks                             |
 | src_tables                   | Source tables that are part of the pipelines                  |
 | target_schema_name           | Target schema name of the pipeline target                     |
 | target_database_name         | Target database name of the pipeline target                   |
| target_table_name            | Target table name of the pipeline target                      |
| target_type                  | This field specifies the type of the target.                  |
| target_base_path             | Target base path of the pipeline target if any                |
| storage_format               | File format in which the data is stored in pipeline target    |
| build_mode                   | overwrite/merge/append                                        |
| natural_keys                 | natural keys specified in the pipeline target                 |
| partition_keys               | partition keys specified in the pipeline target               |
| sort_by_columns              | sort by keys specified in the pipeline target                 |
| target_columns_and_datatypes | Columns and Datatypes of Table at Source                      |
| column_lineage               | Column level lineage for each pipeline target in the pipeline |
| derived_expr_if_any          | Print details on the derived column in pipeline target        |

## Output
The **"table_metadata_extractor_V4.py"** script generates an
output csv file named **"TableMetadata.csv"** in the same directory with all the extracted information from the Infoworks APIs.
It also logs the tables that are failed while extracting information from the Infoworks APIs in a csv file named **"Failed_Tables.csv"** with the exception message causing the failure.

The **"job_metrics_extractor.py"** script generates an output csv file named **"JobMetrics.csv"** containing information of all the jobs executed in Infoworks
for last x minutes.

The **"pipeline_metadata_extractor.py"** script generates an
output csv file named **"PipelineMetadata.csv"** in the same directory with all the extracted information from the Infoworks APIs.

### table_metadata_extractor_V4.py
```
python3 table_metadata_extractor_V4.py --config_file config.json
02-Feb-23 11:06:50 — [ INFO ] — Initiating Request to get Sources 
02-Feb-23 11:06:50 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources?&limit=50
02-Feb-23 11:06:51 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/?limit=50&offset=50
02-Feb-23 11:06:51 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/?limit=50&offset=100
02-Feb-23 11:06:52 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/42d6e843a70bd1b59e2f6c28/configurations/connection
02-Feb-23 11:06:52 — [ INFO ] — Initiating Request to get all tables in source id : 42d6e843a70bd1b59e2f6c28 
02-Feb-23 11:06:52 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/42d6e843a70bd1b59e2f6c28/tables?&limit=50
02-Feb-23 11:06:53 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/fc041084abd4cbe7a9150e4a/configurations/connection
02-Feb-23 11:06:56 — [ INFO ] — Saving Output as TableMetadata.csv 
```

### job_metrics_extractor.py
```
python3 job_metrics_extractor.py --config_file config.json --time_range_for_jobs_in_mins 1500
02-Feb-23 12:03:36 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources?limit=50&offset=0
02-Feb-23 12:03:38 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/?limit=50&offset=50
02-Feb-23 12:03:38 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/?limit=50&offset=100
02-Feb-23 12:03:38 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/admin/jobs?filter={"$and": [{"last_upd": {"$gte": {"$date": "2023-02-01T05:33:38Z"}}},{"entityType": {"$in": ["source"]}}, {"jobType": {"$in": ["source_crawl","source_structured_crawl","source_cdc","source_cdc_merge","export_data","full_load","cdc","source_merge","source_structured_cdc_merge","source_structured_cdc","source_cdc_merge","source_semistructured_cdc_merge","source_semistructured_cdc"]}},{"status":{"$in":["failed","completed","running","pending","canceled","aborted"]}}]}&limit=50&offset=0
02-Feb-23 12:03:39 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/admin/jobs?limit=50&offset=50&filter=%7B%22$and%22:%20%5B%7B%22last_upd%22:%20%7B%22$gte%22:%20%7B%22$date%22:%20%222023-02-01T05:33:38Z%22%7D%7D%7D,%7B%22entityType%22:%20%7B%22$in%22:%20%5B%22source%22%5D%7D%7D,%20%7B%22jobType%22:%20%7B%22$in%22:%20%5B%22source_crawl%22,%22source_structured_crawl%22,%22source_cdc%22,%22source_cdc_merge%22,%22export_data%22,%22full_load%22,%22cdc%22,%22source_merge%22,%22source_structured_cdc_merge%22,%22source_structured_cdc%22,%22source_cdc_merge%22,%22source_semistructured_cdc_merge%22,%22source_semistructured_cdc%22%5D%7D%7D,%7B%22status%22:%7B%22$in%22:%5B%22failed%22,%22completed%22,%22running%22,%22pending%22,%22canceled%22,%22aborted%22%5D%7D%7D%5D%7D
02-Feb-23 12:03:39 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/admin/jobs?filter={"$and": [{"entityId":"00736d098faef1e4f0443762"},{"last_upd": {"$gte": {"$date": "2023-02-01T05:33:38Z"}}},{"jobType": {"$in": ["source_crawl","source_structured_crawl","source_cdc","source_cdc_merge","export_data","full_load","cdc","source_merge","source_structured_cdc_merge","source_structured_cdc","source_cdc_merge","source_semistructured_cdc_merge","source_semistructured_cdc"]}}, {"status":{"$in":["failed","completed","running","pending","canceled","aborted"]}}]}&limit=50&offset=0
02-Feb-23 12:03:39 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/admin/jobs?filter={"$and": [{"entityId":"62d9b123d1d97af9a90a66c5"},{"last_upd": {"$gte": {"$date": "2023-02-01T05:33:38Z"}}},{"jobType": {"$in": ["source_crawl","source_structured_crawl","source_cdc","source_cdc_merge","export_data","full_load","cdc","source_merge","source_structured_cdc_merge","source_structured_cdc","source_cdc_merge","source_semistructured_cdc_merge","source_semistructured_cdc"]}}, {"status":{"$in":["failed","completed","running","pending","canceled","aborted"]}}]}&limit=50&offset=0
02-Feb-23 12:03:40 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/admin/jobs?limit=50&offset=50&filter=%7B%22$and%22:%20%5B%7B%22entityId%22:%2262d9b123d1d97af9a90a66c5%22%7D,%7B%22last_upd%22:%20%7B%22$gte%22:%20%7B%22$date%22:%20%222023-02-01T05:33:38Z%22%7D%7D%7D,%7B%22jobType%22:%20%7B%22$in%22:%20%5B%22source_crawl%22,%22source_structured_crawl%22,%22source_cdc%22,%22source_cdc_merge%22,%22export_data%22,%22full_load%22,%22cdc%22,%22source_merge%22,%22source_structured_cdc_merge%22,%22source_structured_cdc%22,%22source_cdc_merge%22,%22source_semistructured_cdc_merge%22,%22source_semistructured_cdc%22%5D%7D%7D,%20%7B%22status%22:%7B%22$in%22:%5B%22failed%22,%22completed%22,%22running%22,%22pending%22,%22canceled%22,%22aborted%22%5D%7D%7D%5D%7D
02-Feb-23 12:03:40 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/62d9b123d1d97af9a90a66c5/jobs/63dadc2e48ef1c73e0fa8f2d
02-Feb-23 12:03:40 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/admin/jobs?limit=50&offset=50&filter=%7B%22$and%22:%20%5B%7B%22entityId%22:%2200736d098faef1e4f0443762%22%7D,%7B%22last_upd%22:%20%7B%22$gte%22:%20%7B%22$date%22:%20%222023-02-01T05:33:38Z%22%7D%7D%7D,%7B%22jobType%22:%20%7B%22$in%22:%20%5B%22source_crawl%22,%22source_structured_crawl%22,%22source_cdc%22,%22source_cdc_merge%22,%22export_data%22,%22full_load%22,%22cdc%22,%22source_merge%22,%22source_structured_cdc_merge%22,%22source_structured_cdc%22,%22source_cdc_merge%22,%22source_semistructured_cdc_merge%22,%22source_semistructured_cdc%22%5D%7D%7D,%20%7B%22status%22:%7B%22$in%22:%5B%22failed%22,%22completed%22,%22running%22,%22pending%22,%22canceled%22,%22aborted%22%5D%7D%7D%5D%7D
02-Feb-23 12:03:40 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/62d9b123d1d97af9a90a66c5/table-groups/
02-Feb-23 12:03:41 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/00736d098faef1e4f0443762/jobs/63daca3b48ef1c73e0fa8ed6
02-Feb-23 12:03:41 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/62d9b123d1d97af9a90a66c5/jobs/63dadc2e48ef1c73e0fa8f2d/reports/job-metrics?limit=50&offset=0
02-Feb-23 12:03:41 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/00736d098faef1e4f0443762/table-groups/
02-Feb-23 12:03:41 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/00736d098faef1e4f0443762/jobs/63daca3b48ef1c73e0fa8ed6/reports/job-metrics?limit=50&offset=0
02-Feb-23 12:03:42 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/62d9b123d1d97af9a90a66c5/tables/31c35a203dfa3f9c94492701
02-Feb-23 12:03:42 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/00736d098faef1e4f0443762/jobs/63daca3b48ef1c73e0fa8ed6/reports/job-metrics?limit=50&offset=50
02-Feb-23 12:03:42 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/62d9b123d1d97af9a90a66c5/tables/31c35a203dfa3f9c94492701/jobs/63dadc2e48ef1c73e0fa8f2d/reports/sourceFilesPath?limit=50&offset=0
02-Feb-23 12:03:42 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/00736d098faef1e4f0443762/tables/38a78a8e524aa2ae581d1e03
02-Feb-23 12:03:42 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/62d9b123d1d97af9a90a66c5/tables/31c35a203dfa3f9c94492701/jobs/63dadc2e48ef1c73e0fa8f2d/reports/sourceFilesPath?limit=50&offset=50
02-Feb-23 12:03:43 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/00736d098faef1e4f0443762/tables/38a78a8e524aa2ae581d1e03/jobs/63daca3b48ef1c73e0fa8ed6/reports/sourceFilesPath?limit=50&offset=0
02-Feb-23 12:03:43 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/00736d098faef1e4f0443762/jobs/63dacb0b48ef1c73e0fa8ee1
02-Feb-23 12:03:44 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/00736d098faef1e4f0443762/table-groups/
02-Feb-23 12:03:44 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/00736d098faef1e4f0443762/jobs/63dacb0b48ef1c73e0fa8ee1/reports/job-metrics?limit=50&offset=0
02-Feb-23 12:03:45 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/00736d098faef1e4f0443762/jobs/63dacb0b48ef1c73e0fa8ee1/reports/job-metrics?limit=50&offset=50
02-Feb-23 12:03:45 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/admin/jobs?filter={"$and": [{"last_upd": {"$gte": {"$date": "2023-02-01T05:33:38Z"}}},{"jobType": {"$in": ["pipeline_build"]}}, {"status":{"$in":["failed","completed","running","pending","aborted","canceled"]}}]}&limit=50&offset=0
02-Feb-23 12:03:46 — [ INFO ] — Saving Output as JobMetrics.csv
```
### pipeline_metadata_extractor.py
```
python3 pipeline_metadata_extractor.py --config_file config.json --domain_names gd_domain,test,AR_DOMAIN --should_fetch_column_lineage False
08-Mar-23 15:59:21 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/admin/users?&limit=20
08-Mar-23 15:59:23 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/admin/users?limit=20&offset=20
08-Mar-23 15:59:23 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains?&limit=20
08-Mar-23 15:59:23 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/?limit=20&offset=20
08-Mar-23 15:59:24 — [ INFO ] — Domains of interest: {'2a433e1e323d9cd72514a813': 'gd_domain', 'bdfe5575830bc626aa55888c': 'test', 'e7a2b0162978ac12ed5b242d': 'AR_DOMAIN'}
08-Mar-23 15:59:24 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/2a433e1e323d9cd72514a813/pipelines?&limit=20
08-Mar-23 15:59:24 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/2a433e1e323d9cd72514a813/pipelines?limit=20&offset=20
08-Mar-23 15:59:24 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/2a433e1e323d9cd72514a813/pipelines/f9d990827c6e5e25c97ba8fc
08-Mar-23 15:59:25 — [ INFO ] — Successfully got the pipeline f9d990827c6e5e25c97ba8fc details.
08-Mar-23 15:59:36 — [ INFO ] — Saving Output as PipelineMetadata.csv 
```


### environment_metadata_extractor.py

#### Description:

This script is responsible for extracting the Infoworks environments,computes and storage details and export them to CSV file for further analysis.

```shell
python3 environment_metadata_extractor.py --config_file ./config.json --output_directory ./csv/
```
#### Input Arguments

| **Parameter**      | **Description**                                    | Default                                                                     |
|:-------------------|:---------------------------------------------------|:----------------------------------------------------------------------------|
 | `config_file` *    | Config JSON file path along with filename          |                                                                             |
 | `output_directory` | Directory where the Output files should be stored. | ./csv                                                                       |
 | `reports`          | Param to decide whether to generate all reports or individual reports passed to reports params.[{extract_compute_details,extract_environment_details,extract_storage_details} ...] | extract_environment_details,extract_compute_details,extract_storage_details |

**All Parameters marked with * are Required**

#### Fields inside configuration file

| **Parameter**    | **Description**                       |
|:-----------------|:--------------------------------------|
| `host`*          | IP Address of Infoworks VM            |
| `port`*          | 443 for https / 3000 for http         |
| `protocol`*      | http/https                            |
 | `refresh_token`* | Refresh Token from Infoworks Account  |


### Attributes from extract_environment_details.csv
| **Attribute**             | **Description**                                             |
|:--------------------------|:------------------------------------------------------------|
| name                      | Name of the Infoworks environment                           |
| compute_engine            | Type of compute engine used (databricks)                    |
| region                    | Region of Databricks Cluster                                |
| workspace_url             | Databricks Workspace URL                                    |
| secret_name               | Name of the secret where databricks token is stored         |
| platform                  | Name of cloud platform used (azure,aws,gcp)                 |
| warehouse                 | Name of the warehouse in case of Data warehouse environment |
| authentication_properties | Authentication properties                                   |
| additional_params         | Additional parameters set at environment level if any       |
| session_params            | Session parameters set at environment level if any          |
| snowflake_profiles        | List of Snowflake profiles available under this environment |


### Attributes from extract_compute_details.csv
| **Attribute**            | **Description**                                                                   |
|:-------------------------|:----------------------------------------------------------------------------------|
| name                     | Name of the Compute Name                                                          |
| environment_name         | Name of the Infoworks Environment                                                 |
| launch_strategy          | Launch Strategy (persistent/ephemeral)                                            |
| cluster_id               | Databricks Cluster ID                                                             |
| workspace_url            | Databricks Workspace URL                                                          |
| region                   | Databricks Cluster launch region                                                  |
| service_auth_name        | Authentication Service for storing Databricks Token                               |       
| driver_node_type         | Type of Drive node                                                                | 
| worker_node_type         | Type of Worker node                                                               | 
| num_worker_nodes         | Number of worker nodes to start with                                              | 
| max_allowed_workers      | Maximum number of worker nodes that can be launched                               |     
| idle_termination_timeout | Idle time for databricks cluster termination                                      |
| allow_zero_workers       | Boolean value to indicate whther or not zero workers are allowed for this compute |      
| enable_autoscale         | Boolean value to indicate whether or not this compute can autoscale               |   
| advanced_configurations  | Advance configurations if any                                                     |

### Attributes from extract_storage_details.csv
| **Attribute**        | **Description**                                                     |
|:---------------------|:--------------------------------------------------------------------|
| name                 | Name of the Compute Name                                            |
| environment_name     | Name of the Infoworks Environment                                   |
| storage_type         | Type of Storage (dbfs/wasb/adls_gen2)                               |
| scheme               | Type of Scheme (`adl://`/`abfs://`/`abfss://`/`wasb://`/`wasbs://`) |
| storage_account_name | Name of storage account in case of ADLS/Blob Storage                |                     |
| container_name       | Name of container in case of ADLS/Blob Storage                      |                     |
| secret_name          | Name of the secret used for storing account key                     |
| is_default_storage   | Boolean value to indicate if it is default storage or not           |

### adhoc_metrics_report_extractor.py

#### Description:

This script will extract some adhoc reports from Infoworks metadata.

```shell
python3 adhoc_metrics_report_extractor.py --config_file ./config.json --output_directory ./csv/
```
#### Input Arguments

| **Parameter**      | **Description**                                                                                                                                                                                                                                                                       | Default                                                                                                                                                                         |
|:-------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
 | `config_file` *    | Config JSON file path along with filename                                                                                                                                                                                                                                             |                                                                                                                                                                                 |
 | `output_directory` | Directory where the Output files should be stored.                                                                                                                                                                                                                                    | ./csv                                                                                                                                                                           |
 | `reports`          | Param to decide whether to generate all reports or individual reports passed to reports params.[{extract_generic_source_types_usage_report,extract_job_hook_usage,extract_infoworks_artifact_creator,extract_extension_report,extract_workflow_with_bash_node_with_last_runtime} ...] | extract_generic_source_types_usage_report,extract_job_hook_usage,extract_infoworks_artifact_creator,extract_extension_report,extract_workflow_with_bash_node_with_last_runtime  |

**All Parameters marked with * are Required**

#### Fields inside configuration file

| **Parameter**    | **Description**                       |
|:-----------------|:--------------------------------------|
| `host`*          | IP Address of Infoworks VM            |
| `port`*          | 443 for https / 3000 for http         |
| `protocol`*      | http/https                            |
 | `refresh_token`* | Refresh Token from Infoworks Account  |

#### Some sample reports generated by script are as follows:

##### extract_job_hook_usage.csv

This metadata csv contains information about the job hooks(prehook/posthooks) registered in Infoworks.
A job hook in infoworks can be attached either as prehook or posthook job.

| **Attribute**            | **Description**                                   |
|:-------------------------|:--------------------------------------------------|
| job_hook_id              | Id of job hook                                    |
| job_hook_name            | Name of job hook                                  |         
| execution_type           | Execution type of job hook (BASH/PYTHON)          |          
| executable_file_name     | Executable filename                               |    
| sources_using_job_hook   | Infoworks sources which are using this job hook   |  
| pipelines_using_job_hook | Infoworks pipelines which are using this job hook |  
| workflows_using_job_hook | Infoworks workflows which are using this job hook |  
| file_details             | List of files added to this jobhook               |         

##### extract_infoworks_artifact_creator.csv

This script extracts information about who created what artifact along with artifacts type.

| **Attribute**  | **Description**                            |
|:---------------|:-------------------------------------------|
| artifact_id    | ID of the artifact in Infoworks            |
| artifact_name  | Name of the artifact in Infoworks          |
| artifact_type  | Type of artifact (Source/Pipeline/Workflow |
| creator_name   | Artifact creator name                      |
| creator_email  | Artifact creator email                     |
| domain_name    | Artifact Domain (for pipelines/workflows)  |

##### extract_extension_report.csv

This script extracts information about Infoworks extensions along with its usage across different types of artifacts


| **Attribute**             | **Description**                                                                                                     |
|:--------------------------|:--------------------------------------------------------------------------------------------------------------------|
| id                        | Id of extension in Infoworks                                                                                        |
| extension_name            | Name of extension in Infoworks                                                                                      |
| extension_type            | Type of extension (`custom_target`/`Hive_UDF`/`Custom_Deserializer` etc)                                            |       
| execution_type            | Execution Type(`JAVA`/ `PYTHON`)                                                                                    |                                       |       
| transformations           | Transformations under given extension                                                                               |
| ingestion_extension_type  | Ingestion extension type if its is ingestion extension (`source_extension`/`streaming_extension`)                   |
| registered_udf            | UDFs registered if ingestion extension                                                                              | 
| upload_option             | upload option (`fileUpload`,`serverLocation`)                                                                       |   
| server_path               | server path if files are present on the VM.                                                                         |              
| file_details              | files uploaded details                                                                                              |            
| pipelines_using_extension | pipelines which are using this extension if extension is of type `custom_target`                                    |
| domain_name               | Domain under which the extension is accessible                                                                      |           
| sources_using_extension   | sources which are using this extension if ingestion extension type is  (`source_extension`/`streaming_extension`)   |


##### extract_workflow_with_bash_node_with_last_runtime.csv

This script extracts information about Infoworks workflows containing bash node along with their latest workflow run time.

| **Attribute**               | **Description**                                         |
|:----------------------------|:--------------------------------------------------------|
| domain_id                   | Id of Workflow domain in Infoworks                      |
| domain_name                 | Name of Workflow domain in Infoworks                    |
| workflow_id                 | Id of workflow                                          |                      
| workflow_name               | Name of workflow                                        |    
| bash_node                   | Bash node details                                       |    
| custom_image_url            | Custom image URL if the bash node is using custom image |
| workflow_latest_run_id      | Latest workflow run ID                                  |                               
| workflow_latest_run_time    | Latest workflow ID run time                             |

##### extract_generic_source_types_usages_report.csv

This script extracts information about Infoworks generic source types along with its usage across different sources.

| **Attribute**     | **Description**                                                |
|:------------------|:---------------------------------------------------------------|
| name              | Name of the generic source in Infoworks                        |
| id                | Id of generic source in Infoworks                              |       
| dependent_sources | List of sources dependent on or using this generic source type |
                                
##### extract_target_data_connections_report.csv

This script extracts information about Infoworks target data connections and their usage across different pipelines and source tables.

| **Attribute**                   | **Description**                                                                 |
|:--------------------------------|:--------------------------------------------------------------------------------|
| id                              | Id of target data connection in Infoworks                                       |
| name                            | Name of target data connection in Infoworks                                     |
| type                            | Type of target data connection                                                  |                  
| sub_type                        | Subtype of data connection (SQL_SERVER,POSTGRES,SNOWFLAKE etc)                  |                 
| properties                      | JDBC url properties                                                             |                   
| created_at                      | Target data connection creation time                                            |                 
| created_by                      | Target data connection created by which user                                    |                  
| modified_at                     | Target data connection modification time                                        |           
| modified_by                     | Target data connection modified by which user                                   |             
| description                     | Description if any                                                              |               
| pipelines_using_data_connection | list of pipelines using the data connection                                     |
| domain_name                     | list of domains using the data connection                                       |   
| source_tables_using_extension   | list of source tables using data connection in format (table_name, source_name) |
| data_connection_id              | Id of data connection                                                           |    

##### extract_admin_schedules_report.csv

This script extracts information about Infoworks schedules along with schedule user information.

| **Attribute**     | **Description**                                                                       |
|:------------------|:--------------------------------------------------------------------------------------|
| entity_id         | Scheduled entity id                                                                   |
| entity_name       | Scheduled entity name                                                                 |
| schedule_type     | Type of Schedule (mongo-backup,table-group-cdc,workflow-build etc)                    |
| schedule_username | Name of user who scheduled                                                            | 
| schedule_details  | Details of schedule includes(schedule_status,start_date,start_hour,start_min etc)     |
| created_by_name   | Schedule user name                                                                    |
| parent_id         | Parent entity id (domain id for workflow-build, and source_id for table-group-cdc)    |
| domain_name       | Name of the domain that scheduled workflow belongs to                                 |


                                
## Authors
* Sanath Singavarapu
* Abhishek R
* Nitin BS


