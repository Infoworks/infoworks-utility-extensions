# Utilities Directory

## Overview
Welcome to the Utilities directory, a comprehensive solution designed to streamline the administration of Infoworks. This directory houses a collection of tools and scripts that facilitate various administrative tasks such as job metrics monitoring, user and role management, files administration, scheduling, and more.

## Example Utilities

### 1. Infoworks CI/CD

- **Description:** Infoworks CI/CD is a robust utility meticulously crafted to facilitate the smooth promotion of Infoworks artifacts, including sources, pipelines, pipeline groups, and workflows, across various environments, seamlessly progressing from lower to higher stages. Leveraging the power of version control systems such as GitHub, Bitbucket, Azure Repository, and others, Infoworks CI/CD ensures a flexible and collaborative environment. Furthermore, it integrates seamlessly with leading Continuous Delivery tools like Azure Pipelines, GitHub Actions, and more, providing users with a versatile and adaptable solution for efficient deployment processes.
- **Link:** [Infoworks_CICD](./Infoworks_CICD/)

### 2. Infoworks_Metadata_Extractor

- **Description:** This Product Recipe will enable clients to integrate the Infoworks metadata with systems that track metadata. The Product Recipe retrieves static and dynamic information of the Infoworks objects such as table metadata,job metrics,pipeline metadata etc via Infoworks APIs
- **Link:** [Infoworks_Metadata_Extractor](./Infoworks_Metadata_Extractor/)

### 3. Infoworks_External_Job_Scheduler

- **Description:** Infoworks External Job Scheduler is a Python script that triggers various types of infoworks jobs, monitors their progress, and returns their status. It is designed to be flexible and customizable, allowing users to integrate with any external schedulers like Tivoli, Tidal etc.

- **Link:** [Infoworks_External_Job_Scheduler](./Infoworks_External_Job_Scheduler/)

### 4. Infoworks_Streaming_Job_Controller

- **Description:** This script helps to start and stop streaming jobs via Infoworks REST APIs. It also ensures to start the compute before starting the Infoworks Kafka Job.

- **Link:** [Infoworks_Streaming_Job_Controller](./Infoworks_Streaming_Job_Controller/)

### 5. Infoworks_Databricks-Libraries-Uninstall

- **Description:** This tool assists in removing Infoworks-specific libraries/jars on Databricks, facilitating the cleanup of all libraries in the Databricks Cluster when necessary.
- **Link:** [Infoworks_Databricks_Libraries_Uninstall](./Infoworks_Databricks_Libraries_Uninstall/)

### 6. Prehook Scripts

- **Description:** This directory contains the scripts that can be run before bringing in data to Infoworks. These scripts, called pre ingestion job hooks, help perform tasks like encrypting, decrypting, moving, or deleting files. They can also work with other tools. You can write these scripts in either bash or python 3.x. The scripts are executed on the data plane (compute cluster) where the job runs.
- **Link:** [Prehook_Scripts](./Prehook_Scripts/)

    #### Example Prehooks:

    - **iwx_prehook_filewatcher**:
      - **Description**:
      - **Link**: [iwx_prehook_filewatcher](./Prehook_Scripts/iwx_prehook_filewatcher/)
  
    - **iwx_prehook_gpg_decryption**:
      - **Description**:
      - **Link**: [iwx_prehook_gpg_decryption](./Prehook_Scripts/iwx_prehook_gpg_decryption/)
  
    - **iwx_prehook_include_filename_regex_update**:
      - **Description**: This Pre-hook script functions to update the table configurations, incorporating file names based on a regex pattern to reflect the current date. Following the regex replacement, it specifically identifies files that include today's date in their names, exemplified by files like 'file_abc_21-08-2024.txt'
      - **Link**: [iwx_prehook_include_filename_regex_update](./Prehook_Scripts/iwx_prehook_include_filename_regex_update/)

