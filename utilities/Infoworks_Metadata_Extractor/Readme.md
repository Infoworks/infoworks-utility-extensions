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
python3 table_metadata_extractor_V4.py --config_file <Fully qualified path of the configuration file>
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
python3 job_metrics_extractor.py --config_file <Fully qualified path of the configuration file> --time_range_for_jobs_in_mins < TIME_IN_MINS>
```
#### Input Arguments

| **Parameter**                  | **Description**                                                       |
|:-------------------------------|:----------------------------------------------------------------------|
 | `time_range_for_jobs_in_mins`* | Time Range (in Minutes) to extract all the executed jobs in Infoworks |  

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
02-Feb-23 11:06:53 — [ INFO ] — Initiating Request to get all tables in source id : fc041084abd4cbe7a9150e4a 
02-Feb-23 11:06:53 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/fc041084abd4cbe7a9150e4a/tables?&limit=50
02-Feb-23 11:06:53 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/fc041084abd4cbe7a9150e4a/tables?limit=50&offset=50
02-Feb-23 11:06:54 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/13ade36593776fccbd6a0aad/configurations/connection
02-Feb-23 11:06:54 — [ INFO ] — Initiating Request to get all tables in source id : 13ade36593776fccbd6a0aad 
02-Feb-23 11:06:54 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/13ade36593776fccbd6a0aad/tables?&limit=50
02-Feb-23 11:06:55 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/13ade36593776fccbd6a0aad/tables?limit=50&offset=50
02-Feb-23 11:06:55 — [ INFO ] — Calling https://iwx-host.infoworks.technology:443/v3/sources/ac0f2f370e6737f783a9bc7c/configurations/connection
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
08-Mar-23 15:59:25 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/2a433e1e323d9cd72514a813/pipelines/f9d990827c6e5e25c97ba8fc/config-migration
08-Mar-23 15:59:26 — [ INFO ] — Successfully got the pipeline f9d990827c6e5e25c97ba8fc configuration json.
08-Mar-23 15:59:26 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/2a433e1e323d9cd72514a813/pipelines/41640e354219f5703e0785fb
08-Mar-23 15:59:26 — [ INFO ] — Successfully got the pipeline 41640e354219f5703e0785fb details.
08-Mar-23 15:59:26 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/2a433e1e323d9cd72514a813/pipelines/41640e354219f5703e0785fb/config-migration
08-Mar-23 15:59:27 — [ INFO ] — Successfully got the pipeline 41640e354219f5703e0785fb configuration json.
08-Mar-23 15:59:27 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/2a433e1e323d9cd72514a813/pipelines/a69379edcaf6dc8733f55ed5
08-Mar-23 15:59:27 — [ INFO ] — Successfully got the pipeline a69379edcaf6dc8733f55ed5 details.
08-Mar-23 15:59:27 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/2a433e1e323d9cd72514a813/pipelines/a69379edcaf6dc8733f55ed5/config-migration
08-Mar-23 15:59:28 — [ INFO ] — Successfully got the pipeline a69379edcaf6dc8733f55ed5 configuration json.
08-Mar-23 15:59:28 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/bdfe5575830bc626aa55888c/pipelines?&limit=20
08-Mar-23 15:59:28 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/bdfe5575830bc626aa55888c/pipelines?limit=20&offset=20
08-Mar-23 15:59:28 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/bdfe5575830bc626aa55888c/pipelines/28470c0a5be4e8fdd2c0522e
08-Mar-23 15:59:29 — [ INFO ] — Successfully got the pipeline 28470c0a5be4e8fdd2c0522e details.
08-Mar-23 15:59:29 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/bdfe5575830bc626aa55888c/pipelines/28470c0a5be4e8fdd2c0522e/config-migration
08-Mar-23 15:59:29 — [ INFO ] — Successfully got the pipeline 28470c0a5be4e8fdd2c0522e configuration json.
08-Mar-23 15:59:29 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/bdfe5575830bc626aa55888c/pipelines/b89dd5534b534c751b5886c4
08-Mar-23 15:59:30 — [ INFO ] — Successfully got the pipeline b89dd5534b534c751b5886c4 details.
08-Mar-23 15:59:30 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/bdfe5575830bc626aa55888c/pipelines/b89dd5534b534c751b5886c4/config-migration
08-Mar-23 15:59:30 — [ INFO ] — Successfully got the pipeline b89dd5534b534c751b5886c4 configuration json.
08-Mar-23 15:59:30 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/e7a2b0162978ac12ed5b242d/pipelines?&limit=20
08-Mar-23 15:59:30 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/e7a2b0162978ac12ed5b242d/pipelines?limit=20&offset=20
08-Mar-23 15:59:31 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/e7a2b0162978ac12ed5b242d/pipelines/716651e171c79626a5c2cb6e
08-Mar-23 15:59:31 — [ INFO ] — Successfully got the pipeline 716651e171c79626a5c2cb6e details.
08-Mar-23 15:59:31 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/e7a2b0162978ac12ed5b242d/pipelines/716651e171c79626a5c2cb6e/config-migration
08-Mar-23 15:59:32 — [ INFO ] — Successfully got the pipeline 716651e171c79626a5c2cb6e configuration json.
08-Mar-23 15:59:32 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/e7a2b0162978ac12ed5b242d/pipelines/e60f861da306f5ade6fb0647
08-Mar-23 15:59:32 — [ INFO ] — Successfully got the pipeline e60f861da306f5ade6fb0647 details.
08-Mar-23 15:59:32 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/e7a2b0162978ac12ed5b242d/pipelines/e60f861da306f5ade6fb0647/config-migration
08-Mar-23 15:59:33 — [ INFO ] — Successfully got the pipeline e60f861da306f5ade6fb0647 configuration json.
08-Mar-23 15:59:33 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/e7a2b0162978ac12ed5b242d/pipelines/838b7424c0ad539bc031b8e1
08-Mar-23 15:59:34 — [ INFO ] — Successfully got the pipeline 838b7424c0ad539bc031b8e1 details.
08-Mar-23 15:59:34 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/e7a2b0162978ac12ed5b242d/pipelines/838b7424c0ad539bc031b8e1/config-migration
08-Mar-23 15:59:34 — [ INFO ] — Successfully got the pipeline 838b7424c0ad539bc031b8e1 configuration json.
08-Mar-23 15:59:34 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/e7a2b0162978ac12ed5b242d/pipelines/6fa15f29fa442d99b054d342
08-Mar-23 15:59:35 — [ INFO ] — Successfully got the pipeline 6fa15f29fa442d99b054d342 details.
08-Mar-23 15:59:35 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/e7a2b0162978ac12ed5b242d/pipelines/6fa15f29fa442d99b054d342/config-migration
08-Mar-23 15:59:36 — [ INFO ] — Successfully got the pipeline 6fa15f29fa442d99b054d342 configuration json.
08-Mar-23 15:59:36 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/e7a2b0162978ac12ed5b242d/pipelines/5c416d068adfb00bcb74da79
08-Mar-23 15:59:36 — [ INFO ] — Successfully got the pipeline 5c416d068adfb00bcb74da79 details.
08-Mar-23 15:59:36 — [ INFO ] — Calling https://support-test-att.cs.infoworks.cloud:443/v3/domains/e7a2b0162978ac12ed5b242d/pipelines/5c416d068adfb00bcb74da79/config-migration
08-Mar-23 15:59:36 — [ INFO ] — Successfully got the pipeline 5c416d068adfb00bcb74da79 configuration json.
08-Mar-23 15:59:36 — [ INFO ] — Saving Output as PipelineMetadata.csv 
```

## Authors
* Sanath Singavarapu
* Abhishek R
* Nitin BS


