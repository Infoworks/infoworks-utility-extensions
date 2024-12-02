# Purpose:
This script was developed to accelerate the metadata integration between Infoworks and OpenMetadata.
The scope of this integration is to create technical data lineage between different sources and targets for source ingestions and pipelines
Infoworks' customer can use and/or modify the script to fit their needs.

# Data Flow:
The following is diagram depicting the data lineage flow:
![Infoworks - OpenMetadata Integration](/utilities/Infoworks_OpenMetadata/IWXOpenMetadataDataFlow.png?raw=true).

# Pre-requisites:

These are the pre-requisites needed:
1) Python 3.4+ (PyPy supported)
2) Sources and target tables are crawled by OpenMetadata

# Usage:
1) Update config.ini
2) To push the pipeline lineage data from Infoworks to OpenMetadata Server run the below command. This step not only captures the lineage information of the pipeline that is passed, but also the lineage information of any parent pipelines linked to it.
```
python iwx_metadata_extract_to_openmetadata.py --domain_id c5a8c2256c825dd6c605497c --pipeline_id c64c7b8d629428a86c2378af
```
3) To push the source tables’ lineage data from Infoworks to OpenMetadata Server run the below command:
```
python iwx_metadata_extract_to_openmetadata.py --source_table_ids c5a8c2256c825dd6c605497c:c64c7b8d629428a86c2378af
```