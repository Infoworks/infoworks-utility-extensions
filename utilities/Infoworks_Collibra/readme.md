# Purpose:
This script was developed to accelerate the metadata integration between Infoworks and Collibra.
The scope of this integration is to create technical data lineage between different sources and targets for source ingestions and pipelines
Infoworks' customer can use and/or modify the script to fit their needs.

# Data Flow:
The following is diagram depicting the data lineage flow:
![Infoworks - Collibra Integration](/IWXCollibraDataFlow.png?raw=true)

# Pre-requisites:

These are the pre-requisites needed:
1) Python 3.4+ (PyPy supported)
2) Infoworks' Python SDK
3) Collibra Lineage Harvester agent is installed
4) Sources and target tables are crawled by Collibra

# Usage:
1) Update config.ini
2) Execute collibra-lineage.py with arguments iwx_pipeline_id and iwx_domain_id for pipeline lineage or with argument iwx_source_id for source lineage.
3) Place the generated custom lineage json file in the lineage harvester directory and execute lineage-harvester.bat file.