# Infoworks Mass Replicator

## Introduction

Infoworks Mass Replicator is a Python script that automates configuration of table replication in Infoworks Replicator. The script automates the process of creating sources, destinations, definitions and setting up the necessary configuration for replication.

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Setup](#setup)
- [Usage](#usage)
- [Authors](#authors)

## Features

* Create Source, Destination, Replication Definition
* Crawl Source Metadata
* Add Tables to the Replication Definition
* Trigger/Schedule Replication Data Job

## Prerequisites
* Python 3.6
* Infoworks Replicator 4.1.x (not compatible with earlier version of Infoworks)

## Installation
It is recommended to set up a virtual environment and install the required dependencies. 
Below are the commands to do it:

1. Download mass_replicator.py to your local machine and move them into infoworks_mass_replicator directory.
2. Navigate to the project directory:
    ```
    cd infoworks_mass_replicator
    ```
3. Create a new virtual environment using venv:
    ```
    python3 -m venv infoworks_mass_replicator_env
    ```
4. Activate the virtual environment:
    ```
    source infoworks_mass_replicator_env/bin/activate
    ```
5. Install the required packages using pip and the requirements.txt file:
    ```
    pip install -r requirements.txt
    ```

## Setup
Before using the project, Create the replication configuration json file and replication tables csv file.

**replication_config.json**
```
{
  "replicator_instance": {
    "host": < IP Address of Infoworks VM >,
    "port": < 443 for https / 3000 for http >,
    "protocol": < http/https >,
    "refresh_token": < Refresh Token from Infoworks Account >
  },
  "source": {
    "name": < Source Name in Infoworks Replicator >,
    "type": "hive",
    "properties": {
        "hadoop_version": "2.x / 3.x ",
        "hdfs_root": < Hadoop File System Compliant location >,
        "hive_metastore_url": < Hive Metastore connection string to replicate the metadata >,
        "network_throttling": < Throttling Limit in Mbps >,
        "temp_directory": < Temp Directory >,
        "output_directory": < Output Directory >
    },
    "advanced_configurations": [
        {
            "key": < KEY >,
            "value": < VALUE >
        }
    ]
  },
  "destination": {
      "name": < Destination Name in Infoworks Replicator >,
      "type": "hive",
      "properties": {
          "hadoop_version": "2.x",
          "hdfs_root": < Hadoop File System Compliant location >,
          "hive_metastore_url": < Hive Metastore connection string to replicate the metadata >,
          "network_throttling": < Throttling Limit in Mbps >,
          "temp_directory": < Temp Directory >,
          "output_directory": < Output Directory >
      },
      "advanced_configurations": [
          {
              "key": < KEY >,
              "value": < VALUE >
          }
      ]
  },
  "replication": {
    "definition": {
        "name": < Replication Definition Name in Infoworks Replicator >,
        "domain_id": < Domain Id of Infoworks Replicator Domain >,
        "job_bandwidth_mb": < Network Throttling Limit in Mbps >,
        "replication_type": < batch / overwrite >,
        "copy_parallelism_factor": < Data Copy Parallelism Factor >,
        "metastore_parallelism_factor": < Metastore Parallelism Factor >,
        "advanced_configurations": [
            {
                "key": < KEY >,
                "value": < VALUE >
            }
        ]
    },
    "schedule": {
      "entity_type": "replicate_definition",
        "properties": {
            "schedule_status": "enabled",
            "repeat_interval": < hour,minute,day,week,month >,
            "starts_on": < Schedule Start Date >,
            "time_hour": < Hour >,
            "time_min": < Minute >,
            "ends": "off",
            "repeat_every": < Repeat Interval >
        },
      "scheduler_username": < Schedule User >
      "scheduler_auth_token": < Auth Token >
    },
    "trigger_replication_data_job": < true/false >
  }
}
```
**replication_tables.csv**

```
Schema_Name,Table_Name
default,student
```

## Usage
```
python mass_replicator.py --replication_config replication_config.json --replication_tables replication_tables.csv
```

## Authors

* Sanath Singavarapu
