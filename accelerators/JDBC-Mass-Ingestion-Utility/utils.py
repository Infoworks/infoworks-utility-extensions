import subprocess
import sys
import base64
import pkg_resources
import numpy as np
import pandas as pd
import json
required = {'pycryptodomex','pandas'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
from Cryptodome.Cipher import AES
from Cryptodome.Protocol.KDF import PBKDF2
from Cryptodome.Hash import SHA256
import os
cwd = os.path.dirname(os.path.realpath(__file__))

def trim_spaces(item):
    return item.strip()

def check_file_writable(fnm):
    if os.path.exists(fnm):
        # path exists
        if os.path.isfile(fnm): # is it a file or a dir?
            # also works when file is a link and the target is writable
            return os.access(fnm, os.W_OK)
        else:
            return False # path is a dir, so cannot write as a file
    # target does not exist, check perms on parent dir
    pdir = os.path.dirname(fnm)
    if not pdir: pdir = '.'
    # target is creatable if parent dir is writable
    return os.access(pdir, os.W_OK)

def target_table_name(table_name):
    return table_name

def target_schema_name(table_schema):
    return table_schema

def aes_decrypt(encrypted_data):
    try:
        if len(encrypted_data.strip(" ")) == 0:
            return ""
        plain_text = "infoworks"
        ciphertext = base64.b64decode(encrypted_data)
        salt = ciphertext[0:16]
        iv = ciphertext[16:28]
        auth_tag = ciphertext[-16:]
        text = ciphertext[28:-16]
        key = PBKDF2(plain_text, salt, dkLen=256, count=65536, hmac_hash_module=SHA256)
        key = key[0:32]
        cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
        decrypttext = cipher.decrypt_and_verify(text, auth_tag)
        return decrypttext.decode('utf-8')
    except Exception as e:
        # print("failed to decode with aes/gcm/nopadding, defaulting to old method")
        # return aes_decrypt(encrypted_data)
        raise

def panda_strip(x):
    r = []
    for y in x:
        if isinstance(y, str):
            y = y.strip()

        r.append(y)
    return pd.Series(r)

def assign_defaults_and_export_to_csv(resultant,source_type,configuration_file_path,metadata_csv_path):
    configuration_file = open(configuration_file_path, "r")
    configuration_json = json.load(configuration_file)
    resultant = resultant.assign(DERIVE_SPLIT_COLUMN_FUCTION='')
    resultant = resultant.fillna('')
    storage_format = configuration_json.get("ingestion_storage_format","parquet")
    resultant = resultant.assign(STORAGE_FORMAT=storage_format)
    resultant = resultant.assign(INGESTION_STRATEGY='')

    for index, row in resultant.iterrows():
        resultant['INGESTION_STRATEGY'][index] = np.where(
            len(list(map(trim_spaces, resultant['PROBABLE_NATURAL_KEY_COLUMNS'][index].split(',')))) != 0 and len(list(
                map(trim_spaces, [value for value in configuration_json["append_water_marks_columns"] if value in list(
                    map(trim_spaces, resultant['SPLIT_BY_KEY_CANDIDATES'][index].split(',')))]))) != 0,
            'INCREMENTAL_APPEND', 'FULL_REFRESH')
    for index, row in resultant.iterrows():
        resultant['INGESTION_STRATEGY'][index] = np.where(
            len(list(map(trim_spaces, resultant['PROBABLE_NATURAL_KEY_COLUMNS'][index].split(',')))) != 0 and len(list(
                map(trim_spaces, [value for value in configuration_json["merge_water_marks_columns"] if value in list(
                    map(trim_spaces, resultant['SPLIT_BY_KEY_CANDIDATES'][index].split(',')))]))) != 0,
            'INCREMENTAL_MERGE', 'FULL_REFRESH')
    resultant = resultant.assign(WATERMARK_COLUMN='')
    resultant = resultant.assign(TPT_READER_INSTANCES='')
    resultant = resultant.assign(TPT_WRITER_INSTANCES='')
    resultant = resultant.assign(TPT_OR_JDBC='')
    if "sfSchema" in configuration_json.keys():
        resultant['TARGET_SCHEMA_NAME'] = configuration_json.get("sfSchema",
                                                                 resultant['DATABASENAME'].apply(target_schema_name))
    elif "target_schema_name" in configuration_json.keys():
        resultant['TARGET_SCHEMA_NAME'] = configuration_json.get("target_schema_name",
                                                                 resultant['DATABASENAME'].apply(target_schema_name))
    else:
        resultant['TARGET_SCHEMA_NAME'] = resultant['DATABASENAME'].apply(target_schema_name)
    resultant['TARGET_TABLE_NAME'] = resultant['TABLENAME'].apply(target_table_name)
    resultant = resultant.assign(TABLE_GROUP_NAME='')
    resultant = resultant.assign(CONNECTION_QUOTA='')
    resultant = resultant.assign(PARTITION_COLUMN='')
    resultant = resultant.assign(DERIVED_PARTITION='False')
    resultant = resultant.assign(DERIVED_FORMAT='')
    resultant = resultant.assign(SCD_TYPE_2='False')
    resultant = resultant.assign(TPT_WITHOUT_IWX_PROCESSING='False')
    table_type = configuration_json.get("default_table_type","infoworks_managed_table")
    resultant = resultant.assign(TABLE_TYPE=table_type)
    resultant = resultant.assign(USER_MANAGED_TABLE_TARGET_PATH='')
    for index, row in resultant.iterrows():
        if resultant['INGESTION_STRATEGY'][index] in ["INCREMENTAL_MERGE","INCREMENTAL_APPEND"]:
            merge_watermark = [value for value in configuration_json["merge_water_marks_columns"] if value in list(map(trim_spaces,
                                                                                                            [value for value
                                                                                                             in
                                                                                                             configuration_json[
                                                                                                                 "merge_water_marks_columns"]
                                                                                                             if
                                                                                                             value in list(
                                                                                                                 map(trim_spaces,
                                                                                                                     resultant[
                                                                                                                         'SPLIT_BY_KEY_CANDIDATES'][
                                                                                                                         index].split(
                                                                                                                         ',')))]))] if \
            resultant['INGESTION_STRATEGY'][index] == 'INCREMENTAL_MERGE' else ''
            append_watermark = [value for value in configuration_json["append_water_marks_columns"] if
                               value in list(map(trim_spaces,
                                                 [value for value
                                                  in
                                                  configuration_json[
                                                      "append_water_marks_columns"]
                                                  if
                                                  value in list(
                                                      map(trim_spaces,
                                                          resultant[
                                                              'SPLIT_BY_KEY_CANDIDATES'][
                                                              index].split(
                                                              ',')))]))] if \
                resultant['INGESTION_STRATEGY'][index] == 'INCREMENTAL_APPEND' else ''

            resultant['WATERMARK_COLUMN'][index] = ",".join(merge_watermark) if merge_watermark!='' else append_watermark

    resultant = resultant.fillna('')
    try:
        resultant.to_csv(f'{metadata_csv_path}', index=False)
        print(f"Please find the intermediate CSV file at {metadata_csv_path}")
        return True
    except Exception as e:
        print(str(e))
        return False
