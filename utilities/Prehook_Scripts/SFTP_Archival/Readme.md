# SFTP Files Archival

## Introduction
This script will enable clients to archive files of an Infoworks SFTP Source. 
The script runs as a post hook job and moves the files to specified archive location.

## Table of Contents
- [Introduction](#introduction)
- [Setup](#setup)
- [Usage](#usage)
- [Output](#output)
- [Authors](#authors)

## Setup
1. Download sftp_archive_posthook.py and config.json to your local machine.
2. Update the config.json with respective values.

| **Parameter**         | **Description**                      |
|:----------------------|:-------------------------------------|
| `protocol`*           | http/https                           |
| `host`*               | IP Address of Infoworks VM           |
| `port`*               | 443 for https / 3000 for http        |
| `refresh_token`       | Refresh Token from Infoworks Account |
| `private_key_name`    | SFTP Private Key File Name           |
| `archival_base_path`  | Base Path for archiving files        |
3. Login into Infoworks Environment.
4. Navigate to Admin >> Extensions >> Job Hooks >> Add Job Hook
5. Upload sftp_archive_posthook.py, updated config.json and mentioned private key file in config.json
6. Set the Execution Type to Python and sftp_archive_posthook.py as Executable File Name.
7. Attach the created job hook as post hook to sftp infoworks source.

## Output
Files will be archived in the format /YYYYMMDD/JOBID/ within the archival base location.

## Authors
* Sanath Singavarapu
