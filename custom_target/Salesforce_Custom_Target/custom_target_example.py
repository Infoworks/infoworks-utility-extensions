from api.custom_target import CustomTarget
from pyspark.sql import SparkSession
import sys
import subprocess
import pkg_resources
from math import ceil
from uuid import uuid4
required = {'pycryptodomex'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed

if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', '--user', *missing], stdout=subprocess.DEVNULL)

from Cryptodome.Cipher import AES
import base64
from Cryptodome.Random import get_random_bytes
from Cryptodome.Protocol.KDF import PBKDF2
from Cryptodome.Hash import SHA256

class CustomTargetSample(CustomTarget):

    def __init__(self):
        self._spark = None
        self._user_properties = None
        self._processing_context = None
    def aes_decrypt(self,encrypted_data):
        try:
            if len(encrypted_data.strip(" ")) == 0:
                return ""
            password = "infoworks"
            ciphertext  = base64.b64decode(encrypted_data)
            salt = ciphertext[0:16]
            iv = ciphertext[16:28]
            auth_tag = ciphertext[-16:]
            text = ciphertext[28:-16]
            key = PBKDF2(password, salt, dkLen = 256,count=65536,hmac_hash_module=SHA256)
            key = key[0:32]
            cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
            decrypttext = cipher.decrypt_and_verify(text,auth_tag)
            return decrypttext.decode('utf-8')
        except Exception as e:
            #print("failed to decode with aes/gcm/nopadding, defaulting to old method")
            #return aes_decrypt(encrypted_data)
            raise
    def write_to_target(self,input_dataset):
        try:
            print(self._user_properties)
            mode = self._user_properties.get('mode','merge').strip()
            user = self._user_properties.get('user').strip()
            password = self.aes_decrypt(self._user_properties.get('password').strip())
            security_token = self.aes_decrypt(self._user_properties.get('security_token').strip())
            login_url = self._user_properties.get('connection_url','https://test.salesforce.com').strip()
            sf_object =  self._user_properties.get('sf_object').strip().title()
            external_field_name = self._user_properties.get('external_field_name').strip()
            version = self._user_properties.get('version',"43.0").strip()
            bulk=self._user_properties.get('bulk',"True").strip()
            row_count=input_dataset.count()
            staging_temp_table=str(uuid4()).replace("-","")
            staging_table=self._user_properties.get('staging_table',staging_temp_table).strip()
            print("Total rows in the input dataset to Upsert into Salesforce "+str(row_count))
            chunk_size=int(self._user_properties.get('batch_size',"10000").strip())
            if chunk_size>10000 and chunk_size<=0:
                chunk_size=10000
            num_of_chunks=ceil(row_count/chunk_size)
            input_dataset.createOrReplaceTempView("VW_"+staging_table)
            self._spark.sql(f"drop table if exists {staging_table}")
            self._spark.sql(f"create table {staging_table} as (select *,row_number() over (order by 1) as row_num from VW_{staging_table})")
            start=0
            for i in range(0,row_count,chunk_size):
                end=start+chunk_size
                temp_dataset = self._spark.sql(f"select * from {staging_table} where row_num>"+str(start)+" and row_num<="+str(end))
                #input_dataset = input_dataset.exceptAll(temp_dataset)
                if mode.lower() == "merge":
                    temp_dataset.drop("row_num").write().format("com.springml.spark.salesforce").option("username",user).option("password", ''.join([password,security_token])).option("login",login_url).option("sfObject", sf_object).option("externalIdFieldName",external_field_name).option("upsert",True).option("version",version).save()
                elif mode.lower() == "overwrite":
                    input_dataset.write().mode("overwrite").format("com.springml.spark.salesforce").option("username",user).option("password", ''.join([password,security_token])).option("login",login_url).option("sfObject", sf_object).option("version",version).option("bulk",bulk).save()
                start=end
            print("Upserted total of "+str(row_count)+" records into Salesforce")
            print("Successfully Completed writing to Salesforce")
            self._spark.sql(f"drop table if exists {staging_table}")
        except Exception as e :
            raise Exception("Exception while saving dataframe to table")

    def initialise_context(self,spark_session,user_properties,processing_context):
        self._spark = spark_session
        self._user_properties = user_properties
        self._processing_context = processing_context
