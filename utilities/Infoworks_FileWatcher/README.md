# Infoworks File Watcher

## Overview
The Infoworks File Watcher is a Python program designed to monitor files in Azure Data Lake Storage (ADLS) for certain patterns and trigger ingestion jobs in Infoworks. It automates the process of waiting for source files to be fully uploaded before initiating the ingestion process.

## Features
- Monitors files in ADLS for specific patterns.
- Triggers ingestion jobs for tables associated with the monitored files.
- Handles retries and exceptions gracefully.

## Requirements
- Python 3.x
- Azure Storage SDK (`azure-storage-blob`)
- Infoworks SDK (`infoworks-sdk`)

## Prerequisites

Before setting up the Infoworks File Watcher workflow, ensure the following prerequisites are met:

- The bash node should have read access to the ADLS directory where the `.done` files will land.
- Utilize the custom image `iwxcs.azurecr.io/infoworkssdk:v1` within the bash node of the workflow.
- Upload the scripts and configuration files as part of the Infoworks job hook to access them from the workflow. The command to run the script would be:

```bash
python /opt/infoworks/uploads/extensions/<extn_id_canbeextractedfromui>/main.py --config_json_path /opt/infoworks/uploads/extensions/<extn_id_canbeextractedfromui>/config.json --source_ids src_id1,src_id2
```

## Installation
1. Clone the scripts directory from the git repository
2. Install dependencies: `pip install -r requirements.txt`

## Usage

The command to run the filewatcher script would be:

```bash
python main.py --config_json_path config.json --source_ids src_id1,src_id2
```
The sample config.json file can be found alongside the script in the repo.