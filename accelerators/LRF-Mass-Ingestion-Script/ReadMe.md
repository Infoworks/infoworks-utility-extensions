# Mass Ingestion Scripts


## Table of Contents
- [Purpose](#purpose)
- [Introduction](#introduction)
- [Documentation](#documentation)
- [Installation](#installation)
- [Usage](#usage)
- [Example](#example)
- [Authors](#authors)


## Purpose: 

This accelerator is developed to automate the table ingestion configuration in Infoworks based on the existing configuration (control files).
It also includes setting up the table groups and the workflows.


## Introduction

This guide enables you to automate the process of ingesting data from the header-less LRF files to Snowflake Datawarehouse directly. 

This approach would bypass the need for ingesting the LRF files data to Teradata Database in between and then to the Snowflake.
Automation Flow Diagram
The following image depicts the process of creation and automation of CSV source tables, respectively.


![Mass ingestion diagram](/mass_ingestion_automation.png?raw=true)

## Documentation

https://docs.google.com/document/d/1lKyLZMdDrZ8G0DGeOqDm6mVKXmi_0Dlv7L3b6NH2fOs/edit?usp=sharing

## Requirements

Python 3.4+ (PyPy supported)

## Installation
If you have pip access to download python packages from Internet:
	   ``` pip install -r requirements.txt```

### Internet free
* If on Linux :
     ```pip install --no-index --find-links=./wheel_linux infoworkssdk colorama```


* If on Mac :
     ```pip install --no-index --find-links=./wheel_mac infoworkssdk colorama```

## Usage

### Execute extract_schema_fastload.py

This script is responsible for extracting the schema information from Fastload scripts and dump it to table_schema.csv
```
cd structured_csv_automation/
python extract_schema_fastload.py --path_to_ctl_files <path_to_the_folder_with_fastload_ctl_files> --table_details_csv <path to the table details csv>
```


### Execute extract_schema_mload.py
This script is responsible for extracting the schema information from Mload scripts and dump it to table_schema.csv
```
cd fixed_width_automation/
python extract_schema_mload.py --path_to_ctl_files <path_to_the_folder_with_mload_ctl_files> --table_details_csv <path to the table details csv>
```

### Execute CSVSource.py
This script is responsible for configuring Source, creating File mappings and creating tables in Infoworks using the schema extracted from previous script.
```
cd structured_csv_automation/
python CSVSource.py --source_name <source_name_to_create> --refresh_token <your_refresh_token_here> --create_source_extension True --snowflake_metasync_source_id  <snowflake_metadata_syc_source_id>
```

### Exceute FixedWidthSource.py
This script is responsible for configuring Source, creating File mappings and creating tables in Infoworks using the schema extracted from previous script.
```
python FixedWidthSource.py --source_name <source_name_to_create> --refresh_token <your_refresh_token_here> --create_source_extension True --snowflake_metasync_source_id  <snowflake_metadata_syc_source_id> --table_schema_path <path to file with table schema details>
```

### Execute bulk_table_configure.py
This script is used to configure tables in Bulk using table_metadata.csv generated from previous steps.
```
python bulk_table_configure.py --refresh_token <refresh_token> --configure all --metadata_csv_path <path to the tables_metadata.csv> --config_json_path <path to the configurations.json> 
```

