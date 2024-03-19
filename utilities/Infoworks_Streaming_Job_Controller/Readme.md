# Infoworks Streaming Job Controller

## Introduction
The script integrated with Infoworks Python SDK provides a simple solution to start and stop Infoworks streaming jobs via REST APIs.

## Prerequisites
* Python 3.6
* Infoworks 5.3.x (not compatible with earlier version of Infoworks)
* Infoworks Python SDK
  
## Usage
1. Download Infoworks_Streaming_Job_Controller.py to your desired location.
2. Start Infoworks streaming job command.
```
python Infoworks_Streaming_Job_Controller.py start --host <iwx_host> --port <port> --protocol <http/https> --refresh_token <refresh_token> --source_id <iwx_source_id> --compute_template_id <iwx_compute_template_id> --table_ids <comma seperated iwx table ids>
```
3. Stop Infoworks streaming job command.
```
python Infoworks_Streaming_Job_Controller.py stop --host <iwx_host> --port <port> --protocol <http/https> --refresh_token <refresh_token> --source_id <iwx_source_id> --table_ids <comma seperated iwx table ids>
```

#### Input Arguments


| **Parameter**                 | **Description**                                                                                                       |
|:------------------------------|:----------------------------------------------------------------------------------------------------------------------|
| `host`                        | IP Address of Infoworks VM.                                                                                           |
| `port`                        | 443 for https / 3000 for http                                                                                         |
| `protocol`                    | http/https                                                                                                            |
| `refresh_token`               | Refresh Token from Infoworks Account                                                                                  |
| `compute_template_id`         | Identifier of a compute template associated with a data environment                                                   |
| `source_id`                   | Infoworks Source ID                                                                                                   |
| `table_ids`                   | Comma Seperated Infoworks Table IDs to start / stop streaming jobs                                                    |
