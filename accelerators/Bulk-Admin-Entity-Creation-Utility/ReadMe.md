## Bulk Admin Entity Creation Utility

### Problem description:
Customer currently finds it difficult to create lot of admin artifacts like environment profiles,secrets,domains,environments,computes,storages etc.
Expectation is to provide a script that creates the admin entities as per CSV file.

### Prerequisites:
When trying to automate the creation of profiles under the environment section:
1) Click on clone environment of already existing environment. This way you will get a copy of all the aspects of the environment(environment details,compute,storage)
![clone environment image](./img/clone_environment_image.png?raw=true)

2) Once you have a clone of the environment available, you can create a csv file, where each row represents a specific profile information.

For Example:
```csv
name,is_default_profile,warehouse,username,secretname,additional_params,session_params
ramesh,true,"[{ ""name"": ""DEMO_WH"",""description"": """",""is_default_warehouse"": true}]",ramesh,mongo-pg-pwd,"[{""is_active"": true,""is_encrypted"": false,""key"": ""CLIENT_SESSION_KEEP_ALIVE"",""value"": ""TRUE""}]","[{""is_active"": true,""is_encrypted"": false,""key"": ""JDBC_QUERY_RESULT_FORMAT"",""value"": ""JSON""}]"
profile1,false,"[{ ""name"": ""DEMO_WH"",""description"": """",""is_default_warehouse"": false}]",ramesh,mongo-pg-pwd,"[{""is_active"": true,""is_encrypted"": false,""key"": ""CLIENT_SESSION_KEEP_ALIVE"",""value"": ""TRUE""}]","[{""is_active"": true,""is_encrypted"": false,""key"": ""JDBC_QUERY_RESULT_FORMAT"",""value"": ""JSON""}]"
profile2,false,"[{ ""name"": ""DEMO_WH"",""description"": """",""is_default_warehouse"": false}]",ramesh,mongo-pg-pwd,"[{""is_active"": true,""is_encrypted"": false,""key"": ""CLIENT_SESSION_KEEP_ALIVE"",""value"": ""TRUE""}]","[{""is_active"": true,""is_encrypted"": false,""key"": ""JDBC_QUERY_RESULT_FORMAT"",""value"": ""JSON""}]"
profile3,false,"[{ ""name"": ""DEMO_WH"",""description"": """",""is_default_warehouse"": false}]",ramesh,mongo-pg-pwd,"[{""is_active"": true,""is_encrypted"": false,""key"": ""CLIENT_SESSION_KEEP_ALIVE"",""value"": ""TRUE""}]","[{""is_active"": true,""is_encrypted"": false,""key"": ""JDBC_QUERY_RESULT_FORMAT"",""value"": ""JSON""}]"
profile4,false,"[{ ""name"": ""DEMO_WH"",""description"": """",""is_default_warehouse"": false}]",ramesh,mongo-pg-pwd,"[{""is_active"": true,""is_encrypted"": false,""key"": ""CLIENT_SESSION_KEEP_ALIVE"",""value"": ""TRUE""}]","[{""is_active"": true,""is_encrypted"": false,""key"": ""JDBC_QUERY_RESULT_FORMAT"",""value"": ""JSON""}]"
profile5,false,"[{ ""name"": ""DEMO_WH"",""description"": """",""is_default_warehouse"": false}]",ramesh,mongo-pg-pwd,"[{""is_active"": true,""is_encrypted"": false,""key"": ""CLIENT_SESSION_KEEP_ALIVE"",""value"": ""TRUE""}]","[{""is_active"": true,""is_encrypted"": false,""key"": ""JDBC_QUERY_RESULT_FORMAT"",""value"": ""JSON""}]"
```

3)  Environment should be created prior to creation of compute templates and storage.
   
### Infoworks Version support:
5.5 +

### Installation/Usage:

1) Download the package from the below link
```shell
wget -no-check-certificate https://infoworks-releases.s3.amazonaws.com/create_admin_entities_in_bulk.tar.gz
```
2) Untar the package
```shell
tar -xvf create_admin_entities_in_bulk.tar.gz
```
3) Run the python script

```shell
cd create_admin_entities_in_bulk;
python create_admin_entities_in_bulk.py --protocol <http/https> --host <infoworks_hostname> --port <3000/443> --refresh_token <refresh_token> --metadata_csv_path <path to metadata csv file> --admin_entity_type <profile/secret/domain/environment/compute/storage> --environment_id <optional and applicable only for environment entity type>
```

Where,
1) protocol : type of protocol http/https
2) host : hostname of infoworks 
3) port : port where infoworks UI is hosted  <3000/443>
4) refresh_token - Infoworks user refresh_token for API authentication
5) metadata_csv_path - absolute path to the metadata csv file which contains information about profiles for environments / secrets for admin secrets (Schema for the csv can be found in samples provided in next section)
6) admin_entity_type - can be either profile (if trying to configure profiles in bulk for a given environment) or secret( if trying to create admin secrets in bulk) or domain( if trying to create domains in bulk) or environment (if trying to create environments in bulk) or compute (if trying to create compute templates in bulk) or storage (if trying to create storage in bulk) 
7) environment_id (**only for admin_entity_type profile**): pass the id of clone environment under which the profiles are to be created in bulk.


#### Sample for metadata csv of environment profiles:

Expected schema for csv:
```csv
name,is_default_profile,warehouse,username,secretname,additional_params,session_params
```

#### Sample for metadata csv for configuring **secrets in bulk**.

Expected schema for csv:
```csv
name,description,secret_store,secret_name
```

#### Sample for metadata csv for configuring domains in bulk

Expected schema for csv:
```csv
name,description,environment_names,accessible_sources,users
```

#### Sample for metadata csv for configuring environments in bulk

Expected schema for csv:
```csv
name,data_env_type,type,workspace_url,region,databricks_authentication,metadata_configs,connection_url,account_name,snowflake_profiles
```

#### Sample for metadata csv for configuring computes in bulk

Expected schema for csv:
```csv
name,environment_name,launch_strategy,is_default_cluster,is_interactive_cluster,runtime_version,metastore_version,max_allowed_workers,ml_workflow_supported,idle_termination_timeout,worker_node_type,driver_node_type,min_worker_nodes,num_worker_nodes,allow_zero_workers,enable_autoscale,advanced_configurations
```

#### Sample for metadata csv for configuring storages in bulk

Expected schema for csv:
```csv
name,environment_name,storage_type,storage_info,is_default_storage
```

### Example:
**admin_entity_type = profile**
1) copy the id of the snowflake clone environment for which profiles are to be added.
![get_environment_id_image image](./img/get_environment_id_image.png?raw=true)
2) create a csv with the profile parameters to update in bulk
```csv
name,is_default_profile,warehouse,username,secretname,additional_params,session_params
ramesh,true,"[{ ""name"": ""DEMO_WH"",""description"": """",""is_default_warehouse"": true}]",ramesh,mongo-pg-pwd,"[{""is_active"": true,""is_encrypted"": false,""key"": ""CLIENT_SESSION_KEEP_ALIVE"",""value"": ""TRUE""}]","[{""is_active"": true,""is_encrypted"": false,""key"": ""JDBC_QUERY_RESULT_FORMAT"",""value"": ""JSON""}]"
profile1,false,"[{ ""name"": ""DEMO_WH"",""description"": """",""is_default_warehouse"": false}]",ramesh,mongo-pg-pwd,"[{""is_active"": true,""is_encrypted"": false,""key"": ""CLIENT_SESSION_KEEP_ALIVE"",""value"": ""TRUE""}]","[{""is_active"": true,""is_encrypted"": false,""key"": ""JDBC_QUERY_RESULT_FORMAT"",""value"": ""JSON""}]"
profile2,false,"[{ ""name"": ""DEMO_WH"",""description"": """",""is_default_warehouse"": false}]",ramesh,mongo-pg-pwd,"[{""is_active"": true,""is_encrypted"": false,""key"": ""CLIENT_SESSION_KEEP_ALIVE"",""value"": ""TRUE""}]","[{""is_active"": true,""is_encrypted"": false,""key"": ""JDBC_QUERY_RESULT_FORMAT"",""value"": ""JSON""}]"
profile3,false,"[{ ""name"": ""DEMO_WH"",""description"": """",""is_default_warehouse"": false}]",ramesh,mongo-pg-pwd,"[{""is_active"": true,""is_encrypted"": false,""key"": ""CLIENT_SESSION_KEEP_ALIVE"",""value"": ""TRUE""}]","[{""is_active"": true,""is_encrypted"": false,""key"": ""JDBC_QUERY_RESULT_FORMAT"",""value"": ""JSON""}]"
profile4,false,"[{ ""name"": ""DEMO_WH"",""description"": """",""is_default_warehouse"": false}]",ramesh,mongo-pg-pwd,"[{""is_active"": true,""is_encrypted"": false,""key"": ""CLIENT_SESSION_KEEP_ALIVE"",""value"": ""TRUE""}]","[{""is_active"": true,""is_encrypted"": false,""key"": ""JDBC_QUERY_RESULT_FORMAT"",""value"": ""JSON""}]"
profile5,false,"[{ ""name"": ""DEMO_WH"",""description"": """",""is_default_warehouse"": false}]",ramesh,mongo-pg-pwd,"[{""is_active"": true,""is_encrypted"": false,""key"": ""CLIENT_SESSION_KEEP_ALIVE"",""value"": ""TRUE""}]","[{""is_active"": true,""is_encrypted"": false,""key"": ""JDBC_QUERY_RESULT_FORMAT"",""value"": ""JSON""}]"
```

3) Run the python script:
```shell
python create_admin_entities_in_bulk/create_admin_entities_in_bulk.py --protocol https --host att-iwx-pri.infoworks.technology --port 443 --refresh_token zThziQ7MoJJPYAha+U --metadata_csv_path ./environment_profile_update.csv --admin_entity_type environment --environment_id 64f5674639a2d900070e19ae
```

```shell
{'message': 'Environment updated successfully'}
```
4)Validate the profiles on UI
![validate_profle_on_UI image](./img/validate_profle_on_UI.png?raw=true)

**admin_entity_type = environment**

1) Create a CSV with the environment parameters to update in bulk
```csv
name,data_env_type,type,workspace_url,region,databricks_authentication,metadata_configs,connection_url,account_name,snowflake_profiles
Azure_Env_Internal,azure,databricks_internal,https://adb-88.8.azuredatabricks.net/,eastus,"{'authentication_type':'infoworks_managed','password': 'dapib3cd02830fa98'}",,
Azure_Env_External,azure,databricks_external,https://adb-88.8.azuredatabricks.net/,eastus,"{'authentication_type':'external_secret_store','secret_name': 'test_secret1'}","{'connection_url':'abc','driver_name':'abc','user_name':'abc','password':{'password_type': 'external_secret_store', 'secret_name':'test_secret2'}}"
Azure_Env_Unity,azure,databricks_unity_catalog,https://adb-88.8.azuredatabricks.net/,eastus,"{'authentication_type':'service_authentication','authentication_service_name':'cs'}"
API_Snowflake_Environment,snowflake,,https://adb-88.8.azuredatabricks.net/,eastus,"{'authentication_type':'external_secret_store','secret_name': 'test_secret1'}",,https://partner.snowflakecomputing.com,partner,"[{""name"": ""second_profile"", ""is_default_profile"": true, ""warehouse"": [{""name"": ""DEMO_WH"", ""description"": ""val1"", ""is_default_warehouse"": true}], ""authentication_properties"": {""type"": ""default"", ""username"": ""demousr"", ""password"": {""password_type"": ""infoworks_managed"", ""password"": ""abcdefg""}}, ""additional_params"": [{""is_active"": true, ""is_encrypted"": false, ""key"": ""ap1"", ""value"": ""apv1""}, {""is_active"": true, ""is_encrypted"": false, ""key"": ""ap2"", ""value"": ""apv2""}], ""session_params"": [{""is_active"": true, ""is_encrypted"": false, ""key"": ""sp1"", ""value"": ""spv1""}, {""is_active"": true, ""is_encrypted"": false, ""key"": ""sp2"", ""value"": ""spv2""}]}]"
```

2) Run the python script:
```shell
python create_admin_entities_in_bulk/create_admin_entities_in_bulk.py --protocol https --host att-iwx-pri.infoworks.technology --port 443 --refresh_token zThziQ7MoJJPYAha+U --metadata_csv_path ./environment.csv --admin_entity_type environment
```

3) Validate the environment on UI
![validate_environment_on_UI image](./img/validate_environment_on_UI.png?raw=true)

**admin_entity_type = compute**

1) Create a CSV with the compute parameters to update in bulk
```csv
name,environment_name,launch_strategy,is_default_cluster,is_interactive_cluster,runtime_version,metastore_version,max_allowed_workers,ml_workflow_supported,idle_termination_timeout,worker_node_type,driver_node_type,min_worker_nodes,num_worker_nodes,allow_zero_workers,enable_autoscale,advanced_configurations
Default_Persistent_Compute_001,Azure_Env_Internal,persistent,False,False,12.2,,2,False,120,Standard_D4as_v5,Standard_D4as_v5,,2,,,"[{""key"": ""a"", ""value"": ""b""},
{""key"": ""c"", ""value"": ""d""},
{""key"": ""b"", ""value"": ""c""}]"
Persistent_Compute_01,Azure_Env_External,persistent,False,True,12.2,2.3.9,2,False,180,Standard_D4as_v5,Standard_D4as_v5,,2,,,
Ephemeral_Compute_04,Azure_Env_External,ephemeral,True,False,12.2,2.3.9,2,True,,Standard_D4as_v5,Standard_D4as_v5,,2,True,,
Peristent_Compute,Azure_Env_Unity,persistent,True,True,14.3,,2,False,180,Standard_D4as_v5,Standard_D4as_v5,,2,,,
Default_Persistent_Compute,API_Snowflake_Environment,persistent,False,True,11.3,,2,False,200,Standard_D4as_v5,Standard_D4as_v5,,2,,,
Default_Ephemeral_Compute_New,API_Snowflake_Environment,ephemeral,True,False,11.3,,2,True,,Standard_D8as_v5,Standard_D8as_v5,1,2,,True,
```
2) Run the python script:
```shell
python create_admin_entities_in_bulk/create_admin_entities_in_bulk.py --protocol https --host att-iwx-pri.infoworks.technology --port 443 --refresh_token zThziQ7MoJJPYAha+U --metadata_csv_path ./computes.csv --admin_entity_type compute
```

3) Validate the computes on UI
![validate_compute_on_UI image](./img/validate_compute_on_UI.png?raw=true)

**admin_entity_type = storage**

1)  Create a CSV with the storage parameters to update in bulk
```csv
name,environment_name,storage_type,storage_info,is_default_storage
DBFS_Storage,Azure_Env_Internal,dbfs,,True
ADLS_Access_Storage,Azure_Env_Internal,adls_gen2,"{""access_scheme"":""abfss://"",""authentication_mechanism"":""access_key"",
  ""storage_account_name"": ""iwx_cicd"", ""container_name"": ""csv-test"", ""password_type"": ""external_secret_store"", ""secret_name"": ""test_secret1""
}",False
ADLS_Gen2_Storage,Azure_Env_External,adls_gen2,"{""access_scheme"":""abfs://"",""authentication_mechanism"":""service_principal_with_service_credentials"",
  ""storage_account_name"": ""iwx_cicd"", ""container_name"": ""csv-test"",
  ""application_id"": ""e1238680-a450-678d-910e-111135b7560f"", ""directory_id"": ""a598762c-9543-4210-a999-12682934205fe"", ""password_type"": ""external_secret_store"", 
  ""secret_name"": ""test_secret1""
}",True
ADLS_Storage,API_Snowflake_Environment,adls_gen2,"{""access_scheme"":""abfs://"",""authentication_mechanism"":""service_principal_with_service_credentials"",
  ""storage_account_name"": ""iwx_cicd"", ""container_name"": ""csv-test"",
  ""application_id"": ""e1238680-a450-678d-910e-111135b7560f"", ""directory_id"": ""a598762c-9543-4210-a999-12682934205fe"", ""password_type"": ""external_secret_store"", 
  ""secret_name"": ""test_secret1""
}",True
ADLS_Gen2_Storage,Azure_Env_Unity,adls_gen2,"{""access_scheme"":""abfss://"",""authentication_mechanism"":""service_principal_with_service_credentials"",
  ""storage_account_name"": ""iwx_cicd"", ""container_name"": ""csv-test"",
  ""application_id"": ""e1238680-a450-678d-910e-111135b7560f"", ""directory_id"": ""a598762c-9543-4210-a999-12682934205fe"", ""storage_credential_name"": ""abcd"",
  ""password_type"": ""external_secret_store"", ""secret_name"": ""test_secret1""
}",False
```

2) Run the python script:
```shell
python create_admin_entities_in_bulk/create_admin_entities_in_bulk.py --protocol https --host att-iwx-pri.infoworks.technology --port 443 --refresh_token zThziQ7MoJJPYAha+U --metadata_csv_path ./storages.csv --admin_entity_type storage
```

3) Validate the storages on UI
![validate_storage_on_UI image](./img/validate_storage_on_UI.png?raw=true)

**Output**
1. Entity type Environment generates an overall_environment_status.csv file containing create/update status of each environment.
2. Entity type Compute generates an overall_compute_status.csv file containing create/update status of each compute template.
3. Entity type Storage generates an overall_storage_status.csv file containing create/update status of each storage.
4. Entity type Secrets generates an overall_secrets_status.csv file containing create/update status of each secret.
5. Entity type Domains generates an overall_domains_status.csv file containing create/update status of each domain.

## Authors
Nitin BS(nitin.bs@infoworks.io), Sanath Singavarapu(sanath.singavarapu@infoworks.io)

