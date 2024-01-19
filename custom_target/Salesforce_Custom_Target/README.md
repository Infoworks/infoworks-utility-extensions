# Infoworks Salesforce Custom Target

## Table of Contents
- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Authors](#authors)

## Introduction 

Infoworks custom target solution to write the data to Salesforce in UPSERT/OVERWRITE mode.

## Prerequisites

A Salesforce object must be present beforehand and must have the relevant fields added to the object along with the External Id column(set as external Id type on salesforce).

## Scenario:

External systems(like sql server) will be generating the upsert record by attaching an external Id to each of the records,based on which the upserts will happen on salesforce.
![clone environment image](./Infoworks_salesforce_custom_target.png?raw=true)

## Setup:

### For VM Based Installation:

1) External System must be configured as a source in Infoworks
2) Once the data is in the data lake, create a pipeline with the above mentioned source table and a custom target Node.
![clone environment image](./infoworks_table_screenshot.png?raw=true)

In order to use the custom target node, we need a pipeline extension created.
- Login to Infoworks VM as infoworks-user
- mkdir -p /opt/infoworks/scripts/salesforce_cust_target
- Download the python egg file from the below link and place it under /opt/infoworks/scripts/salesforce_cust_target/

```shell
wget —-no-check-certificate https://infoworks-releases.s3.amazonaws.com/salesforce_cust_target-0.1-py3.6.egg
```
- Run source env.sh
```shell
source /opt/infoworks/bin/env.sh
```

- Run below python command
```shell
python -m easy_install salesforce_cust_target-0.1-py3.6.egg
```

3) Now that you have the egg file in place, You need to create a pipeline extension as follows:

Extensions > Pipeline Extension > Create pipeline Extension
![clone environment image](./iwx_pipeline_extension.png?raw=true)
Configure as shown below:
![clone environment image](./edit_pipeline_extension.png?raw=true)

4) Go to Manage Domains > Edit the domain with the pipeline > click on manage artifacts > Add the pipeline extension to this domain

![clone environment image](./add_pipeline_extension_to_domain.png?raw=true)


5) Now the pipeline extension must be visible in the Custom target node. Select the same as shown below.
![clone environment image](./pipeline_extension_dropdown.png?raw=true)

Add the below mentioned properties, all of them are mandatory,and provide the corresponding salesforce details.

![clone environment image](./custom_target_properties.png?raw=true)

Where,
- mode : can be set to merge/overwrite (merge by default)
- user : SFDC username
- password : SFDC password in encrypted format as shown in next section.
- security_token :SFDC security token in encrypted format as shown in next section.
- connection_url : connection url of your SFDC.(For sandbox:https://test.salesforce.com ,For Production:  https://login.salesforce.com
For more information: https://help.salesforce.com/s/articleView?id=000324395&type=1
)
- sf_object : Name of the SFDC object to be updated.
- external_field_name : External field name on which the upsert should happen on SFDC.
- version : version of the Salesforce api to use(default 43.0)
- batch_size: Number of records for each batch(default 10000)
**Note** :
SAML is not supported.


**Note**:
Password and security_token must be encrypted using the infoworks encryption strategy mentioned below. Ensure to remove any b’’ on the result of encryption.
```shell
source $IW_HOME/bin/env.sh
```
```shell
$IW_HOME/apricot-meteor/infoworks_python/infoworks/bin/infoworks_security.sh -encrypt -p <yourpassword>
```
6) create directory for salesforce custom target jars
```shell
mkdir -p /opt/infoworks/custom_target_salesforce_jars/
```

7) Download the tar file from the below link and place it under /opt/infoworks/custom_target_salesforce_jars/

```shell
wget –no-check-certificate https://infoworks-releases.s3.amazonaws.com/custom_target_salesforce_jars.tar.gz
```

8) cd /opt/infoworks/custom_target_salesforce_jars/
```shell
tar -xvf custom_target_salesforce_jars.tar.gz
```
9) Edit $IW_HOME/conf/conf.properties

And append :/opt/infoworks/custom_target_salesforce_jars/* to below config entries.


### Before Change:
```ini
dt_batch_classpath=/opt/infoworks/lib/extras/dt/*:/opt/infoworks/lib/dt/jars/*:/opt/infoworks/lib/dt/libs/*:/opt/infoworks/lib/platform/vertx/*
dt_interactive_classpath=:/opt/infoworks/lib/platform/vertx/*:/opt/infoworks/lib/dt/interactive/*:/opt/infoworks/lib/extras/dt/*:/opt/infoworks/lib/dt/jars/*:/opt/infoworks/lib/dt/libs/*
```
```ini
dt_libs_classpath=/opt/infoworks/lib/dt/jars/*:/opt/infoworks/lib/dt/libs/*
```
### After Change:
```ini
dt_batch_classpath=/opt/infoworks/lib/extras/dt/*:/opt/infoworks/lib/dt/jars/*:/opt/infoworks/lib/dt/libs/*:/opt/infoworks/lib/platform/vertx/*
dt_interactive_classpath=:/opt/infoworks/lib/platform/vertx/*:/opt/infoworks/lib/dt/interactive/*:/opt/infoworks/lib/extras/dt/*:/opt/infoworks/lib/dt/jars/*:/opt/infoworks/lib/dt/libs/*:/opt/infoworks/custom_target_salesforce_jars/*

dt_libs_classpath=/opt/infoworks/lib/dt/jars/*:/opt/infoworks/lib/dt/libs/*:/opt/infoworks/custom_target_salesforce_jars/*

```


Make sure that the output columns of the custom_target contains the External_field_name column provided,and only the columns which are present in salesforce. Any new column must be removed from the target node before building the pipeline.

Run the pipeline and validate the results on the Salesforce object.


### For GKE Based installation:

Step 1-5 similar to VM installation.

**Note**:

Password and security_token must be encrypted using the infoworks encryption strategy mentioned below. Ensure to remove any b’’ on the result of encryption.
```shell
 $IW_HOME/iw-k8s-installer/infoworks_security/infoworks_security.sh -encrypt -p <password>
 ```


6) Login to the Bastion host where you could access GKE and list pods


7) Navigate to infoworks home directory and download the dependent jars tar package 
```shell
cd /opt/infoworks/
```

Download the tar file from the below link and place it under /opt/infoworks/
```shell
wget --no-check-certificate https://infoworks-releases.s3.amazonaws.com/custom_target_salesforce_jars.tar.gz
```

Note:
Ensure that the logged in user has write permission to /opt/infoworks ($IW_HOME)

8) Navigate to directory and untar the package
```shell
 cd /opt/infoworks/
 tar -xvf custom_target_salesforce_jars.tar.gz
 rm custom_target_salesforce_jars.tar.gz
```

9) You will have a new directory with the name salesforce_spark created

10) Copy all the jars from local /opt/infoworks/salesforce_spark/
To ```shell/opt/infoworks/uploads/lib/external-dependencies/common/salesforce_jars/```
on pod
```shell
kubectl cp /opt/infoworks/salesforce_spark/ <your_ingestion_pod_name>:/opt/infoworks/uploads/lib/external-dependencies/common/ -c ingestion -n <your_namespace>
```

11) Validate if the jars are present under /opt/infoworks/uploads/lib/external-dependencies/common/salesforce_spark/ of pod
```shell
kubectl exec -it <your-ingestion-pod-name> -n <your-namespace> -- ls -l /opt/infoworks/uploads/lib/external-dependencies/common/salesforce_spark/
total 5416
-rw-r--r-- 1 jboss 1000  236126 Dec 22 04:35 ST4-4.0.7.jar
-rw-r--r-- 1 jboss 1000  445288 Dec 22 04:35 antlr-2.7.7.jar
-rw-r--r-- 1 jboss 1000  167735 Dec 22 04:35 antlr-runtime-3.5.jar
-rw-r--r-- 1 jboss 1000 1453046 Dec 22 04:35 byte-buddy-0.6.14.jar
-rw-r--r-- 1 jboss 1000 1047268 Dec 22 04:35 force-partner-api-40.0.0.jar
-rw-r--r-- 1 jboss 1000  300971 Dec 22 04:35 force-wsc-40.0.0.jar
-rw-r--r-- 1 jboss 1000   98678 Dec 22 04:35 jackson-dataformat-xml-2.9.7.jar
-rw-r--r-- 1 jboss 1000  746500 Dec 22 04:35 mockito-core-2.0.31-beta.jar
-rw-r--r-- 1 jboss 1000   41755 Dec 22 04:35 objenesis-2.1.jar
-rw-r--r-- 1 jboss 1000   74585 Dec 22 04:35 salesforce-wave-api-1.0.10.jar
-rw-r--r-- 1 jboss 1000   84586 Dec 22 04:35 spark-salesforce_2.12-1.1.4.jar
-rw-r--r-- 1 jboss 1000   23346 Dec 22 04:35 stax-api-1.0-2.jar
-rw-r--r-- 1 jboss 1000  161867 Dec 22 04:35 stax2-api-3.1.4.jar
-rw-r--r-- 1 jboss 1000  148627 Dec 22 04:35 stringtemplate-3.2.1.jar
-rw-r--r-- 1 jboss 1000  485895 Dec 22 04:35 woodstox-core-asl-4.4.0.jar
```

12) Edit the conf-properties-configmap
Assuming the values.yaml file is stored in ${IW_HOME}/iw-k8s-installer/infoworks/values.yaml, edit the file ${IW_HOME}/iw-k8s-installer/infoworks/templates/configs/common/conf-properties-configmap.yaml and append :/opt/infoworks/uploads/lib/external-dependencies/common/salesforce_spark/* to below config entries.

### Before Change:
```ini
dt_batch_classpath={{ .Values.iwhome }}/lib/extras/dt/*:{{ .Values.iwhome }}/lib/dt/jars/*:{{ .Values.iwhome }}/lib/dt/libs/*:{{ .Values.iwhome }}/lib/platform/vertx/*

dt_libs_classpath={{ .Values.iwhome }}/lib/dt/jars/*:{{ .Values.iwhome }}/lib/dt/libs/*
```

### After Change:

```ini
dt_batch_classpath={{ .Values.iwhome }}/lib/extras/dt/*:{{ .Values.iwhome }}/lib/dt/jars/*:{{ .Values.iwhome }}/lib/dt/libs/*:{{ .Values.iwhome }}/lib/platform/vertx/*:/opt/infoworks/uploads/lib/external-dependencies/common/salesforce_spark/*

dt_libs_classpath={{ .Values.iwhome }}/lib/dt/jars/*:{{ .Values.iwhome }}/lib/dt/libs/*:/opt/infoworks/uploads/lib/external-dependencies/common/salesforce_spark/*
```

Run the helm upgrade commands from ${IW_HOME}/iw-k8s-installer/

```shell
helm upgrade <release_name> infoworks/ --values infoworks/values.yaml -n <namespace>
```


13) Restart the dt and config service pods for the changes to get reflected.
```shell
kubectl rollout restart deployment <release-name>-dt <release-name>-config-service -n <your-namespace>
```

Make sure that the output columns of the custom_target contains the External_field_name column provided,and only the columns which are present in salesforce. Any new column must be removed from the target node before building the pipeline.

Run the pipeline and validate the results on the Salesforce object.




## Authors:
Nitin BS (nitin.bs@infoworks.io)


