## Bulk Admin Entity Creation Utility

### Problem description:
Customer currently finds it difficult to create lot of admin artifacts like environment profiles,secrets,domains etc.
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
python create_admin_entities_in_bulk.py --protocol <http/https> --host <infoworks_hostname> --port <3000/443> --refresh_token <refresh_token> --metadata_csv_path <path to metadata csv file> --admin_entity_type <environment/secret/domain> --environment_id <optional and applicable only for environment entity type>
```

Where,
1) protocol : type of protocol http/https
2) host : hostname of infoworks 
3) port : port where infoworks UI is hosted  <3000/443>
4) refresh_token - Infoworks user refresh_token for API authentication
5) metadata_csv_path - absolute path to the metadata csv file which contains information about profiles for environments / secrets for admin secrets (Schema for the csv can be found in samples provided in next section)
6) admin_entity_type - can be either environment (if trying to configure profiles in bulk for a given environment) or secret( if trying to create admin secrets in bulk) or domain( if trying to create domains in bulk) 
7) environment_id (**only for admin_entity_type environment**): pass the id of clone environment under which the profiles are to be created in bulk.


#### Sample for metadata csv of environments:

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

### Example:
admin_entity_type = environment
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
python create_admin_entities_in_bulk/create_admin_entities_in_bulk.py --protocol https --host att-iwx-pri.infoworks.technology --port 443 --refresh_token zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2 --metadata_csv_path ./environment_profile_update.csv --admin_entity_type environment --environment_id 64f5674639a2d900070e19ae
```

```shell
{'message': 'Environment updated successfully'}
```
4)Validate the profiles on UI
![validate_profle_on_UI image](./img/validate_profle_on_UI.png?raw=true)


## Authors
Nitin BS(nitin.bs@infoworks.io)

