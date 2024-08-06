## Problem description:
Since the 6.0 upgrade of Infoworks, a new feature has been available in the product that allows users to associate domains with secrets so that all the developers belonging to a particular domain can see only the secrets to which they have access.
As a default behavior post-upgrade, all the domains still have access to all the secrets, unless explicitly assigned. Customers would find it difficult to update individual domains, with the secrets, as they have around 400+ secrets already in use.
This utility would help automatically figure out the domain secrets association based on its usage in the product and assign the respective domain domains with their accessible secrets.

## Usage instructions:

1. Download the package
```shell
wget --no-check-certificate https://infoworks-releases.s3.amazonaws.com/Infoworks_map_domains_to_secrets.tar.gz
```

2. tar unzip the package
```shell
tar -xvf Infoworks_map_domains_to_secrets.tar.gz
```
3. Navigate to Infoworks_map_domains_to_secrets directory
```shell
cd Infoworks_map_domains_to_secrets
```
4. Configure the config.json file with the basic information required for making the API calls.
Fields inside the configuration file

| **Parameter**   | **Description**                                      |
|:----------------|:-----------------------------------------------------|
| host*           | IP Address of Infoworks VM                           |
| port*           | 443 for https / 3000 for http                        | 
| protocol*       | http/https                                           | 
| refresh_token*  | Refresh Token of Admin user  from Infoworks Account  | 

All Parameters marked with * are required.

**Note** : Refresh token must be of an admin user who has full visibility of domains across all projects in Infoworks, otherwise only the domains that are accessible to the user will get updated.

5. Run the script
```shell
python3 associate_secrets_to_domains.py --config_file config.json
```

|  **Input Arguments** | **Description**                           |  **Default** |
|:---------------------|:------------------------------------------|:-------------|
| config_file *        | Config JSON file path along with filename |              |
All Arguments marked with * are required

### Logging/ Debugging:

The log for this script will be available in the same directory as associate_secrets_to_domains.log.

Ignored safe errors if any will be logged at the very end of the script.

