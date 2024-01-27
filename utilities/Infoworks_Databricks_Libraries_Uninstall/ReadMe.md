# Utility to uninstall databricks libraries

## Table of Contents
- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
- [Example](#example)
- [Authors](#authors)

## Introduction: 
Build a script that deletes the old libraries on databricks cluster.
Currently, a user has to manually delete the infoworks libraries on databricks for interactive clusters after each upgrade. Expectation is to have a script that uninstalls the libraries on behalf of the end user.

## Installation:
```sh
wget --no-check-certificate https://infoworks-releases.s3.amazonaws.com/uninstall_databricks_libraries.tar.gz
```

## Usage:

1) Download the package
```sh
wget --no-check-certificate https://infoworks-releases.s3.amazonaws.com/uninstall_databricks_libraries.tar.gz
```
2) Untar the downloaded package
```sh
tar -xvf uninstall_databricks_libraries.tar.gz
```

3) Navigate to package directory
```sh
cd uninstall_databricks_libraries
```

4) Edit config.ini file
```ini
[iwx_details]
protocol=
host=
port=
refresh_token=
prefix=
[databricks_details]
db_account_url=
[service_principal]
tenant_id=
client_id=
```

Where,
[iwx_details] section covers the details of the Infoworks installation
	**protocol** - http/https
	**host** - host of infoworks UI
	**port**  - port of infoworks (UI 3000 for http/443 for https)
	**refresh_token** - refresh_token for making restapi call
	**prefix** - prefix of the libraries to be uninstalled <ingestion/platform/etc>
	(* for all the libraries)
	
	For example: 
	
	_If you want to delete all the libraries under ingestion folder of "dbfs:////iw_g0erpczs/ingestion/_opt_infoworks_lib_ingestion_spark_3x_2.12_data-connectors/spark-connector-commons.jar", you need to pass ingestion as prefix_
	_If you want to delete all libraries under ingestion/platform ,then they need to pass –prefix ingestion,platform_

[databricks_details] section covers the details of databricks account like account_url
	**db_account_url** - databricks account url

[service_principal] section covers the details of service principal for oauth authentication.
	**tenant_id**- the registered application’s tenant ID.
	**client_id** - the registered application’s client ID.
		

## Script parameters:
```sh
python3 uninstall_databricks_libraries.py --auth_type <db_token/service_principal> –env_ids <comma separated list of env_ids>
```

where,
	**auth_type** (*required*) - indicates the auth type to authenticate via databricks token or using service principal <db_token/service principal>

	**env_ids** (*optional*) - you can pass the list of env ids if you want to uninstall the libraries of clusters of specific environment in infoworks. By default all environments will be considered if this parameter is not passed.



## Example:

1) I have uploaded two jars 
dbfs:////iw_g0erpczs/test_uninstall_libraries/warehouse-connector.jar,
dbfs:////iw_g0erpczs/test_uninstall_libraries/test-platform-common.jar


2) I will run the script by passing test_uninstall_libraries as prefix:
python3 uninstall_databricks_libraries.py --auth_type service_principal

### Script output:
```sh
python3 uninstall_databricks_libraries.py --auth_type service_principal --env_ids ed1c6973a2d2046f1fe93a20
```
```sh
Enter the client secret: **<enter your client secret in this prompt>**

url https://<host>:443/v3/admin/environment/ed1c6973a2d2046f1fe93a20/environment-interactive-clusters
Environment ID in Infoworks :  ed1c6973a2d2046f1fe93a20
Compute Name in Infoworks :  Default_Persistent_Compute_Template_clone
Cluster ID :  0228-053549-vcs3y4zr
Libraries to uninstall:
{'jar': 'dbfs:////iw_g0erpczs/test_uninstall_libraries/test-platform-common.jar'}
{'jar': 'dbfs:////iw_g0erpczs/test_uninstall_libraries/warehouse-connector.jar'}
Uninstalling the libraries...
Libraries Uninstalled successfully for cluster 0228-053549-vcs3y4zr!
{}
Restarting the cluster..
Cluster restarted successfully!
{}
             environment_id  ...                            cluster_restart_message
0  ed1c6973a2d2046f1fe93a20  ...                    Cluster restarted successfully!
1  ed1c6973a2d2046f1fe93a20  ...        Cluster 0320-105621-ihg7erzy does not exist
2  ed1c6973a2d2046f1fe93a20  ...  Cluster 0321-092809-ulhiuoz7 is in unexpected ...
3  ed1c6973a2d2046f1fe93a20  ...        Cluster 0324-052441-2y4te3r9 does not exist
4  ed1c6973a2d2046f1fe93a20  ...  Cluster 0330-092008-hdtaiy9k is in unexpected ...

[5 rows x 7 columns]
please find the cluster libraries uninstall status for each cluster here: /home/infoworks/uninstall_databricks_libraries/uninstall_library_output.csv
```

4) The script will generate a csv file named uninstall_library_output.csv in same directory as script, and will contain the detailed information about the uninstall and restart success/failure status for each cluster.
5) Script uninstalled the libraries based on prefix in config.ini for all the clusters and restarted the clusters.

## Authors
Nitin BS (nitin.bs@infoworks.io)