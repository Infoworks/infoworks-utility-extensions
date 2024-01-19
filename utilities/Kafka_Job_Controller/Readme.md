# Kafka Job Controller

## Introduction
This script helps to start and stop streaming jobs via Infoworks REST APIs

## Prerequisites
* Python 3.6
* Infoworks 5.3.x (not compatible with earlier version of Infoworks)
* Infoworks Python SDK
  
## Usage
```
python Kafka_Jobs_Controller.py start --host <iwx_host> --port <port> --protocol <http/https> --refresh_token <refresh_token> --source_id <iwx_source_id> --compute_template_id <iwx_compute_template_id> --table_ids <comma seperated iwx table ids>
```

```
python Kafka_Jobs_Controller.py stop --host <iwx_host> --port <port> --protocol <http/https> --refresh_token <refresh_token> --source_id <iwx_source_id> --table_ids <comma seperated iwx table ids>
```
