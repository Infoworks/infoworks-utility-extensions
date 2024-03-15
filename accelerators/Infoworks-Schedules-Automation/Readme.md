# Schedules Automation

## Introduction
This Script automates the creation of schedules for table groups and workflows in Infoworks.


## Table of Contents
- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Output](#output)
- [Authors](#authors)

## Prerequisites
* Python 3.6
* Infoworks 5.3.x (not compatible with earlier version of Infoworks)

## Usage

### schedules_automation.py
```
python3 schedules_automation.py --config_file <Fully qualified path of the configuration file> --schedules_info_file <Fully qualified path of the schedules info file>
```
#### Fields inside configuration file

| **Parameter**    | **Description**                       |
|:-----------------|:--------------------------------------|
| `host`*          | IP Address of Infoworks VM            |
| `port`*          | 443 for https / 3000 for http         |
| `protocol`*      | http/https                            |
 | `refresh_token`* | Refresh Token from Infoworks Account  |

**All Parameters marked with * are Required**

#### Fields inside schedules info file


| **Parameter**              | **Description**                                    |
|:---------------------------|:---------------------------------------------------|
| `source_id`                | ID of Infoworks Source                             |
| `table_group_id`           | ID of Infoworks Table Group                        |
| `domain_id`                | ID of Infoworks Domain                             |
 | `workflow_id`              | ID of Infoworks Workflow                           |
| `start_date`               | Start date of the Schedule                         |
| `start_hour`               | Start hour of the Schedule                         |
| `start_min`                | Start min of the Schedule                          |
 | `repeat_interval_unit`     | Time unit for the repeat interval                  |
| `repeat_interval_measure`  | Measure to specify the repeat interval             |
| `repeat_on_last_day`       | Boolean whether to repeat on last day of the month |
| `specified_days`           | Specific days for schedule to execute              |
 | `ends`                     | Boolean whether a schedule has end date            |
| `end_date`                 | End date of the Schedule                           |
| `end_hour`                 | End hour of the Schedule                           |
| `end_min`                  | End min of the Schedule                            |
 | `is_custom_job`            | Boolean whether the schedule is custom             |
 | `custom_job_details`       | Details of the custom Schedule                     |


## Output

The script configure schedules for the given table groups / workflows in schedules info file. 
User configured in the configuration file will be configured as schedule user for the schedule.

### schedules_automation.py
```
09-Oct-23 18:04:14 — [ INFO ] — Configuring schedule for workflow : 64ef3d00164d9b56be2de918 - domain id : 64ad413c3a553b7d1e16e896
09-Oct-23 18:04:14 — [ INFO ] — Calling https://iwx.infoworks.technology:443/v3/domains/64ad413c3a553b7d1e16e896/workflows/64ef3d00164d9b56be2de918/schedules/user
09-Oct-23 18:04:15 — [ INFO ] — Successfully updated workflow 64ef3d00164d9b56be2de918 schedule user.
09-Oct-23 18:04:15 — [ INFO ] — Calling https://iwx.infoworks.technology:443/v3/domains/64ad413c3a553b7d1e16e896/workflows/64ef3d00164d9b56be2de918/schedules/enable
09-Oct-23 18:04:16 — [ INFO ] — Successfully enabled schedule for the workflow 64ef3d00164d9b56be2de918.
09-Oct-23 18:04:16 — [ INFO ] — Configuring schedule for table group : 64d61f1b86f8502b88b0b382 - source id : 64c9f07f5b3b07f82f2ce148
09-Oct-23 18:04:16 — [ INFO ] — Calling https://iwx.infoworks.technology:443/v3/sources/64c9f07f5b3b07f82f2ce148/table-groups/64d61f1b86f8502b88b0b382/schedules/user
09-Oct-23 18:04:16 — [ INFO ] — Successfully configured table group 64d61f1b86f8502b88b0b382 schedule user.
09-Oct-23 18:04:16 — [ INFO ] — Calling https://iwx.infoworks.technology:443/v3/sources/64c9f07f5b3b07f82f2ce148/table-groups/64d61f1b86f8502b88b0b382/schedules/enable
09-Oct-23 18:04:16 — [ INFO ] — Successfully enabled schedule for the table group 64d61f1b86f8502b88b0b382.
```

## Authors
* Sanath Singavarapu
