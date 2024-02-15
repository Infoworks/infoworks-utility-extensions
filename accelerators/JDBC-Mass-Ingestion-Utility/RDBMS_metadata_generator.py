# Usage:
# python3 RDBMS_metadata_generator.py --configuration_file_path <configuration.json path along with filename> <source_type> --host <hostname/ip> --port <port> --user <username> --password <encrypted_password> --schemas <comma seperated list of schemas/databases> --metadata_csv_file <path to metadata csv file that will be generated along with filename>
import argparse
import os
import subprocess
import sys
import traceback
from abc import ABC, abstractmethod
import pandas as pd
import pkg_resources
from utils import aes_decrypt, assign_defaults_and_export_to_csv, panda_strip, check_file_writable

cwd = os.path.dirname(os.path.realpath(__file__))


class RDBMSSource(ABC):
    @abstractmethod
    def install_and_download_dependencies(self):
        pass

    @abstractmethod
    def get_connection_object(self):
        pass

    @abstractmethod
    def generate_sql_and_get_results(self):
        pass


class TeradataSource(RDBMSSource):
    def __init__(self, host, port, user, password, schema):
        self.host = host
        self.port = port
        self.user = user
        self.password = aes_decrypt(password)
        self.schema = schema

    def install_and_download_dependencies(self):
        required = {'teradatasql'}
        installed = {pkg.key for pkg in pkg_resources.working_set}
        missing = required - installed
        if missing:
            python = sys.executable
            subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

    def get_connection_object(self):
        try:
            import teradatasql
            connection = teradatasql.connect(host=self.host, dbs_port=self.port, user=self.user, password=self.password)
            return connection
        except Exception as e:
            print("Unable to establish the connection to Teradata")
            print(str(e))
            traceback.format_exc()
            exit(-100)

    def generate_sql_and_get_results(self):
        schema = ','.join("'" + schema_name + "'" for schema_name in list(map(str.upper, self.schema)))
        schema_query = ""
        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f"and DataBaseName in ({schema})"
        connection = self.get_connection_object()
        # query to fetch all tables size
        # table_size_query = f"SELECT A.DataBaseName as DATABASENAME,A.TableName as TABLENAME,B.TableKind as TABLEKIND,CAST(SUM(A.currentperm)/(1024*1024) AS DECIMAL(18,5)) AS TABLE_SIZE_MB FROM dbc.tablesize as A right outer join DBC.TablesV as B on A.DataBaseName= B.DatabaseName and A.TableName=B.TableName where (B.TableKind = 'T' or B.TableKind = 'V') {schema_query} group by A.DataBaseName,A.TableName,B.TableKind";
        table_size_query = f"Select DataBaseName as DATABASENAME,TableName as TABLENAME FROM DBC.TablesV where (TableKind = 'T' or TableKind = 'V') {schema_query} group by DataBaseName,TableName,TableKind"
        # query to fetch the primary keys
        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f"and t.DataBaseName in ({schema})"

        tables_primary_keys = f"""SELECT  t.DataBaseName as DATABASENAME,
                t.TableName as TABLENAME,
                i.IndexName as INDEXNAME,
                TRIM(TRAILING ','
                    FROM XMLAGG(i.columnName || ','
                         ORDER BY i.columnPosition)(varchar(10000))) AS PROBABLE_NATURAL_KEY_COLUMNS
        FROM    DBC.TablesV t
        LEFT JOIN   DBC.IndicesV i
        ON  t.DatabaseName = i.DataBaseName
        AND t.TableName = i.TableName
        AND (i.IndexType = 'K' or i.IndexType='P' or i.IndexType = 'A'  or i.IndexType = 'U'  or i.IndexType = 'Q' or i.IndexNumber=1)
        WHERE   (t.TableKind = 'T' or t.TableKind = 'V')
        AND t.DataBaseName NOT IN ('All', 'Crashdumps', 'DBC', 'dbcmngr',
            'Default', 'External_AP', 'EXTUSER', 'LockLogShredder', 'PUBLIC',
            'Sys_Calendar', 'SysAdmin', 'SYSBAR', 'SYSJDBC', 'SYSLIB',
            'SystemFe', 'SYSUDTLIB', 'SYSUIF', 'TD_SERVER_DB', 'TDStats',
            'TD_SYSGPL', 'TD_SYSXML', 'TDMaps', 'TDPUSER', 'TDQCD',
            'tdwm', 'SQLJ', 'TD_SYSFNLIB', 'SYSSPATIAL') {schema_query}
        GROUP BY t.DatabaseName,
                 t.TableName,
                 i.IndexName
        ORDER BY t.DatabaseName,
                 t.TableName;"""
        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f"and DataBaseName in ({schema})"

        split_by_keys = f"""SELECT  DataBaseName as DATABASENAME,
                TableName as TABLENAME,
                TRIM(TRAILING ',' FROM XMLAGG(ColumnName || ',')(varchar(10000))) AS SPLIT_BY_KEY_CANDIDATES
        FROM    DBC.ColumnsV
        WHERE   ColumnType in ('DA', 'AT', 'TS', 'TZ', 'SZ', 'YR', 'YM', 'MO',
                'DY', 'DH', 'DM', 'DS', 'HR', 'HM', 'HS', 'MI', 'MS', 'SC')
                AND DataBaseName NOT IN ('All', 'Crashdumps', 'DBC', 'dbcmngr',
                'Default', 'External_AP', 'EXTUSER', 'LockLogShredder', 'PUBLIC',
                'Sys_Calendar', 'SysAdmin', 'SYSBAR', 'SYSJDBC', 'SYSLIB',
                'SystemFe', 'SYSUDTLIB', 'SYSUIF', 'TD_SERVER_DB',
                'TD_SYSGPL', 'TD_SYSXML', 'TDMaps', 'TDPUSER', 'TDQCD',
                'TDStats', 'tdwm', 'SQLJ', 'SYSSPATIAL','TD_SYSFNLIB') {schema_query}
        GROUP BY DataBaseName,
                    TableName
        ORDER BY    DataBaseName,
                    TableName;"""
        # Fetch the data from Teradata using Pandas Dataframe
        pd_table_size = pd.read_sql(table_size_query, connection)

        pd_primary_keys = pd.read_sql(tables_primary_keys, connection)
        pd_split_by_keys = pd.read_sql(split_by_keys, connection)
        print(pd_split_by_keys)
        pd_primary_keys = pd_primary_keys.apply(lambda x: panda_strip(x))
        pd_table_size = pd_table_size.apply(lambda x: panda_strip(x))
        pd_split_by_keys = pd_split_by_keys.apply(lambda x: panda_strip(x))
        print("pd_table_size\n", pd_table_size.info(verbose=True))
        # print(pd_table_size)
        print("pd_primary_keys\n", pd_primary_keys.info(verbose=True))
        # print(pd_primary_keys)
        print("pd_split_by_keys\n", pd_split_by_keys.info(verbose=True))
        resultant_temp = pd_table_size.merge(
            pd_primary_keys[['DATABASENAME', 'TABLENAME', "PROBABLE_NATURAL_KEY_COLUMNS"]], how="left")
        resultant = resultant_temp.merge(pd_split_by_keys[['DATABASENAME', 'TABLENAME', 'SPLIT_BY_KEY_CANDIDATES']],
                                         how="left")
        return resultant


class OracleSource(RDBMSSource):
    def __init__(self, host, port, user, password, schema, service):
        self.host = host
        self.port = port
        self.user = user
        self.password = aes_decrypt(password)
        self.schema = schema
        self.service = service

    def install_and_download_dependencies(self):
        required = {'cx_Oracle'}
        installed = {pkg.key for pkg in pkg_resources.working_set}
        missing = required - installed
        if missing:
            python = sys.executable
            subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

    def get_connection_object(self):
        try:
            import cx_Oracle
            connection = cx_Oracle.connect(f'{self.user}/{self.password}@{self.host}:{self.port}/{self.service}')
            return connection
        except Exception as e:
            print("Unable to establish the connection to Oracle")
            print(str(e))
            traceback.format_exc()
            exit(-100)

    def generate_sql_and_get_results(self):
        import cx_Oracle
        def handle_clob(item):
            if type(item) == cx_Oracle.LOB:
                return item.read()
            else:
                return str(item)

        schema = ','.join("'" + schema_name + "'" for schema_name in list(map(str.upper, self.schema)))
        schema_query = ""
        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f"and owner IN ({schema})"
        connection = self.get_connection_object()
        all_tables = f"""select object_name as tablename,owner as databasename,object_type as TABLE_TYPE
        from all_objects t
        where (object_type = 'TABLE' or object_type = 'VIEW')
        {schema_query} order by object_name"""
        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f"and acc.owner IN ({schema})"

        table_primary_keys = f"""select acc.owner as DataBaseName,
               acc.constraint_name,
               acc.table_name as TableName,
               LISTAGG(acc.column_name,',')
                      WITHIN GROUP (order by acc.position) as probable_natural_key_columns
        from sys.all_constraints con
        join sys.all_cons_columns acc on con.owner = acc.owner
             and con.constraint_name = acc.constraint_name
        where (con.constraint_type = 'P' or con.constraint_type = 'U')
              and acc.owner not in 
              ('ANONYMOUS','CTXSYS','DBSNMP','EXFSYS','LBACSYS',
             'MDSYS','MGMT_VIEW','OLAPSYS','OWBSYS','ORDPLUGINS','ORDSYS','OUTLN',
             'SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM','TSMSYS','WK_TEST',
             'WKPROXY','WMSYS','XDB','APEX_040000','APEX_040200','APEX_PUBLIC_USER',
             'DIP','FLOWS_30000','FLOWS_FILES','MDDATA','ORACLE_OCM','XS$NULL',
             'SPATIAL_CSW_ADMIN_USR','SPATIAL_WFS_ADMIN_USR','WKSYS', 'PUBLIC')
              and acc.table_name not like 'BIN$%' {schema_query}
        group by acc.owner,
                 acc.table_name,
                 acc.constraint_name
        order by acc.owner,
                 acc.constraint_name"""

        primary_key_df = pd.read_sql(table_primary_keys, con=connection)
        primary_key_df.set_index(['TABLENAME', 'DATABASENAME'], drop=True)
        # print(df.to_string(index=False))
        primary_key_df.reset_index(drop=True)
        print("primary_key_df\n", primary_key_df.info(verbose=True))

        schema_query = ""
        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f"and col.owner IN ({schema})"

        split_by_key = f"""
        select col.owner as DataBaseName,
               col.table_name as TableName,
                rtrim(xmlagg(xmlelement(e,col.column_name,',').extract('//text()') order by col.column_id).getClobVal(),',') as split_by_key_candidates
        from sys.all_tab_cols col
        join sys.all_tables tab on col.owner = tab.owner
                                and col.table_name = tab.table_name
        where (data_type in ('DATE')
              or col.data_type like 'TIMESTAMP%')
              and col.owner not in ('ANONYMOUS','CTXSYS','DBSNMP','EXFSYS', 
              'MDSYS', 'MGMT_VIEW','OLAPSYS','OWBSYS','ORDPLUGINS', 'ORDSYS',
              'SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM', 'TSMSYS','WK_TEST',
              'WKPROXY','WMSYS','XDB','APEX_040000', 'APEX_PUBLIC_USER','DIP', 
              'FLOWS_30000','FLOWS_FILES','MDDATA', 'ORACLE_OCM', 'XS$NULL',
              'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR', 'PUBLIC',
              'OUTLN', 'WKSYS', 'APEX_040200', 'LBACSYS') {schema_query}
        group by col.owner,
                 col.table_name
        order by col.owner,
                 col.table_name
                 """
        split_by_key_df = pd.read_sql(split_by_key, con=connection)
        split_by_key_df.reset_index(drop=True)
        split_by_key_df.set_index(['TABLENAME', 'DATABASENAME'], drop=True)
        print("split_by_key_df\n", split_by_key_df.info(verbose=True))
        print(split_by_key_df.columns)

        all_tables_df = pd.read_sql(all_tables, con=connection)
        all_tables_df.reset_index(drop=True)
        all_tables_df.set_index(['TABLENAME', 'DATABASENAME'], drop=True)
        # print(all_tables_df)
        primary_key_df = all_tables_df.merge(
            primary_key_df[['DATABASENAME', 'TABLENAME', 'PROBABLE_NATURAL_KEY_COLUMNS']], how="left")
        # print(primary_key_df)
        resultant = primary_key_df.merge(split_by_key_df[['DATABASENAME', 'TABLENAME', 'SPLIT_BY_KEY_CANDIDATES']],
                                         how="left")
        resultant['SPLIT_BY_KEY_CANDIDATES'] = resultant['SPLIT_BY_KEY_CANDIDATES'].apply(handle_clob)
        resultant = resultant.fillna('')
        return resultant


class VerticaSource(RDBMSSource):
    def __init__(self, host, port, user, password, schema):
        self.host = host
        self.port = port
        self.user = user
        self.password = aes_decrypt(password)
        self.schema = schema

    def install_and_download_dependencies(self):
        required = {'vertica-python==0.11.0', 'sqlalchemy-vertica', 'pandas==1.1.0'}
        installed = {pkg.key for pkg in pkg_resources.working_set}
        missing = required - installed
        if missing:
            python = sys.executable
            subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

    def get_connection_object(self):
        try:
            import sqlalchemy as sa
            engine = sa.create_engine(f'vertica+vertica_python://{self.user}:{self.password}@{self.host}:{self.port}')
            connection = engine.connect()
            return connection
        except Exception as e:
            print("Unable to establish the connection to Vertica")
            print(str(e))
            traceback.format_exc()
            exit(-100)

    def generate_sql_and_get_results(self):
        pd.options.mode.chained_assignment = None
        schema_query = ""
        schema = ','.join("'" + schema_name + "'" for schema_name in list(map(str.upper, self.schema)))
        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f"where table_schema in ({schema})"
        connection = self.get_connection_object()
        # query to fetch all tables size
        table_size_query = f"(select table_schema as DATABASENAME,table_name as TABLENAME from v_catalog.tables {schema_query} order by table_schema,table_name) union (select table_schema as DATABASENAME,table_name as TABLENAME from v_catalog.views {schema_query});";

        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f" and table_schema in ({schema})"

        # query to fetch the primary keys
        tables_primary_keys = f"select table_schema as DATABASENAME,table_name as TABLENAME,listagg(column_name) as PROBABLE_NATURAL_KEY_COLUMNS from v_catalog.constraint_columns where (constraint_type='p' or constraint_type='u'){schema_query} group by table_schema,table_name"

        split_by_keys = f"select table_schema as DATABASENAME,table_name as TABLENAME,listagg(column_name) as SPLIT_BY_KEY_CANDIDATES from v_catalog.columns c where (data_type like 'interval%' or data_type like 'date%' or data_type like 'time%'){schema_query} group by table_schema,table_name"

        # Fetch the data from Teradata using Pandas Dataframe
        pd_table_size = pd.read_sql(table_size_query, connection)

        pd_primary_keys = pd.read_sql(tables_primary_keys, connection)

        pd_split_by_keys = pd.read_sql(split_by_keys, connection)
        print(pd_split_by_keys)
        pd_primary_keys = pd_primary_keys.apply(lambda x: panda_strip(x))
        pd_table_size = pd_table_size.apply(lambda x: panda_strip(x))
        pd_split_by_keys = pd_split_by_keys.apply(lambda x: panda_strip(x))

        print("pd_table_size\n", pd_table_size.info(verbose=True))
        # print(pd_table_size)
        print("pd_primary_keys\n", pd_primary_keys.info(verbose=True))

        # print(pd_primary_keys)
        print("pd_split_by_keys\n", pd_split_by_keys.info(verbose=True))

        resultant_temp = pd_table_size.merge(
            pd_primary_keys[['DATABASENAME', 'TABLENAME', "PROBABLE_NATURAL_KEY_COLUMNS"]], how="left")
        resultant = resultant_temp.merge(pd_split_by_keys[['DATABASENAME', 'TABLENAME', 'SPLIT_BY_KEY_CANDIDATES']],
                                         how="left")
        return resultant


class MySQLSource(RDBMSSource):
    def __init__(self, host, port, user, password, schema, skip_views):
        self.host = host
        self.port = port
        self.user = user
        self.password = aes_decrypt(password)
        self.schema = schema
        self.skip_views = skip_views

    def install_and_download_dependencies(self):
        required = {'pymysql'}
        installed = {pkg.key for pkg in pkg_resources.working_set}
        missing = required - installed
        if missing:
            python = sys.executable
            subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

    def get_connection_object(self):
        try:
            import pymysql
            ssl = {'ssl_verify_identity': 'true'}
            connection = pymysql.connect(host=self.host, port=int(self.port), user=self.user, passwd=self.password,
                                         connect_timeout=5, ssl=ssl)
            return connection
        except Exception as e:
            print("Unable to establish the connection to MYSQL")
            print(str(e))
            traceback.format_exc()
            exit(-100)

    def generate_sql_and_get_results(self):
        schema_query = ""
        schema = ','.join("'" + schema_name + "'" for schema_name in self.schema)
        skip_views = eval(self.skip_views)
        view_query = ""
        if not skip_views:
            view_query = " or tab.table_type='SYSTEM VIEW'"
        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f"and tab.table_schema IN ({schema})"
        connection = self.get_connection_object()
        all_tables = f"""select table_schema as DATABASENAME,
        table_name as TABLENAME,tab.table_type as TABLE_OR_VIEW from information_schema.tables tab
        where (tab.table_type = 'BASE TABLE'{view_query})
        {schema_query}
        order by tab.table_schema, tab.table_name;"""
        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f"and  tab.table_schema IN ({schema})"

        table_primary_keys = f"""select tab.table_schema as DATABASENAME,
        tab.table_name as TABLENAME,
        group_concat(distinct sta.column_name order by sta.column_name) as PROBABLE_NATURAL_KEY_COLUMNS
        from information_schema.tables as tab
        inner join information_schema.statistics as sta
        	on sta.table_schema = tab.table_schema
            and sta.table_name = tab.table_name
            and sta.index_name = 'primary'
        where (tab.table_type = 'BASE TABLE'{view_query})
        {schema_query}
        group by tab.table_name,tab.table_schema
        order by tab.table_name,tab.table_schema;"""
        primary_key_df = pd.read_sql(table_primary_keys, con=connection)
        primary_key_df.set_index(['TABLENAME', 'DATABASENAME'], drop=True)
        # print(df.to_string(index=False))
        primary_key_df.reset_index(drop=True)
        print("primary_key_df\n", primary_key_df.info(verbose=True))
        print(primary_key_df)
        schema_query = ""
        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f"and  tab.table_schema IN ({schema})"

        split_by_key = f"""select tab.table_schema as DATABASENAME,
        tab.table_name as TABLENAME,
        group_concat(distinct col.column_name) as SPLIT_BY_KEY_CANDIDATES
        from information_schema.tables as tab
        left join information_schema.columns as col
            on col.table_schema = tab.table_schema
            and col.table_name = tab.table_name
        where (tab.table_type = 'BASE TABLE'{view_query})
        {schema_query}
        and col.data_type in ('date','timestamp','int')
        group by tab.table_schema,tab.table_name
        order by tab.table_schema,tab.table_name"""
        split_by_key_df = pd.read_sql(split_by_key, con=connection)
        split_by_key_df.reset_index(drop=True)
        split_by_key_df.set_index(['TABLENAME', 'DATABASENAME'], drop=True)
        print("split_by_key_df\n", split_by_key_df.info(verbose=True))
        print(split_by_key_df.columns)

        all_tables_df = pd.read_sql(all_tables, con=connection)
        all_tables_df.reset_index(drop=True)
        all_tables_df.set_index(['TABLENAME', 'DATABASENAME'], drop=True)
        print(all_tables_df)
        primary_key_df = all_tables_df.merge(
            primary_key_df[['DATABASENAME', 'TABLENAME', 'PROBABLE_NATURAL_KEY_COLUMNS']], how="left")
        # print(primary_key_df)

        resultant = primary_key_df.merge(split_by_key_df[['DATABASENAME', 'TABLENAME', 'SPLIT_BY_KEY_CANDIDATES']],
                                         how="left")
        resultant = resultant.fillna('')
        resultant['TABLE_OR_VIEW'] = resultant['TABLE_TYPE'].str.replace('BASE ', '')
        return resultant


class NetezzaSource(RDBMSSource):
    def __init__(self, host, port, user, password, db, schema, driver_path):
        self.host = host
        self.port = port
        self.user = user
        self.password = aes_decrypt(password)
        self.db = db
        self.schema = schema
        self.driver_path = driver_path

    def install_and_download_dependencies(self):
        required = {'JayDeBeApi==1.2.3'}
        installed = {pkg.key for pkg in pkg_resources.working_set}
        missing = required - installed
        if missing:
            python = sys.executable
            subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

    def get_connection_object(self):
        try:
            import jaydebeapi
            jdbc_driver_name = "org.netezza.Driver"
            jdbc_driver_loc = os.path.join(self.driver_path)
            connection_string = 'jdbc:netezza://' + self.host + ':' + self.port + '/' + self.db
            connection = jaydebeapi.connect(jdbc_driver_name, connection_string,
                                            {'user': self.user, 'password': self.password},
                                            jars=jdbc_driver_loc)
            return connection
        except Exception as e:
            print("Unable to establish the connection to Netezza")
            print(str(e))
            traceback.format_exc()
            exit(-100)

    def generate_sql_and_get_results(self):
        schema_query = ""
        schema = ','.join("'" + schema_name + "'" for schema_name in list(map(str.upper, self.schema)))
        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f" schema in ({schema}) and owner not in ('ADMIN')"

        def listagg(L):
            return ",".join(L)

        connection = self.get_connection_object()
        table_size_query = f"""
        select
        schema as DATABASENAME,
        TABLENAME,
        used_bytes/pow(1024,3) as TABLE_SIZE_IN_GB
        from _v_table_storage_stat
        where {schema_query}"""

        # query to fetch the primary keys
        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f"and schema in ({schema})"

        tables_primary_keys = f"""
        SELECT  schema as DATABASENAME
                , constraintname
                , relation as TABLENAME
                , conseq as seq
                , attname as PROBABLE_NATURAL_KEY_COLUMNS
        FROM _v_relation_keydata
        where (contype='p' or contype='u') {schema_query}
        order by relation, conseq
        """

        if (schema == ""):
            schema_query = ""
        else:
            schema_query = f"and schema in ({schema})"

        split_by_keys = f"""
        select
        schema as DATABASENAME,table_name as TABLENAME,column_name as SPLIT_BY_KEY_CANDIDATES
        from _V_SYS_COLUMNS
        where TYPE_NAME in ('TIMESTAMP','DATE') {schema_query}
        """
        # Fetch the data from Teradata using Pandas Dataframe
        pd_table_size = pd.read_sql(table_size_query, connection)

        pd_primary_keys = pd.read_sql(tables_primary_keys, connection)
        # print(pd_primary_keys)
        pd_primary_keys = pd_primary_keys.groupby(['TABLENAME', 'DATABASENAME'])['PROBABLE_NATURAL_KEY_COLUMNS'].apply(
            listagg).reset_index(name="PROBABLE_NATURAL_KEY_COLUMNS")
        # print(pd_primary_keys)
        if pd_primary_keys.empty:
            pd_primary_keys = pd.DataFrame(columns=['TABLENAME', 'DATABASENAME', 'PROBABLE_NATURAL_KEY_COLUMNS'])

        pd_split_by_keys = pd.read_sql(split_by_keys, connection)
        pd_split_by_keys = pd_split_by_keys.groupby(['TABLENAME', 'DATABASENAME'])['SPLIT_BY_KEY_CANDIDATES'].apply(
            listagg).reset_index(name="SPLIT_BY_KEY_CANDIDATES")

        print(pd_split_by_keys)
        pd_primary_keys = pd_primary_keys.apply(lambda x: panda_strip(x))
        pd_table_size = pd_table_size.apply(lambda x: panda_strip(x))
        pd_split_by_keys = pd_split_by_keys.apply(lambda x: panda_strip(x))

        print("pd_table_size\n", pd_table_size.info(verbose=True))
        # print(pd_table_size)
        print("pd_primary_keys\n", pd_primary_keys.info(verbose=True))

        # print(pd_primary_keys)
        print("pd_split_by_keys\n", pd_split_by_keys.info(verbose=True))

        resultant_temp = pd_table_size.merge(
            pd_primary_keys[['DATABASENAME', 'TABLENAME', "PROBABLE_NATURAL_KEY_COLUMNS"]], how="left")
        resultant = resultant_temp.merge(pd_split_by_keys[['DATABASENAME', 'TABLENAME', 'SPLIT_BY_KEY_CANDIDATES']],
                                         how="left")
        resultant = resultant.fillna('')
        return resultant


class SnowflakeSource(RDBMSSource):
    def __init__(self, account_name, user, password, warehouse, database, schemas, tables, skip_views):
        self.account_name = account_name
        self.user = user
        self.password = aes_decrypt(password)
        self.warehouse = warehouse
        self.database = database
        self.schemas = schemas
        self.tables = tables
        self.skip_views = skip_views

    def install_and_download_dependencies(self):
        required = {'snowflake-connector-python==2.7.4'}
        installed = {pkg.key for pkg in pkg_resources.working_set}
        missing = required - installed
        if missing:
            python = sys.executable
            subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

    def get_connection_object(self):
        try:
            import snowflake.connector

            config = {
                'user': self.user,
                'password': self.password,
                'account': self.account_name,
                'warehouse': self.warehouse,
                'database': self.database,
            }
            connection = snowflake.connector.connect(**config)
            return connection
        except Exception as e:
            print("Unable to establish the connection to snowflake")
            print(str(e))
            traceback.format_exc()
            exit(-100)

    def generate_sql_and_get_results(self):
        schema = ','.join("'" + schema_name + "'" for schema_name in list(map(str.upper, self.schemas)))
        tables = ','.join("'" + table_name + "'" for table_name in list(map(str.upper, self.tables)))

        schema_query = "" if schema == "" else f"and TABLE_SCHEMA in ({schema})"
        tables_query = "" if tables == "" else f"and TABLE_NAME in ({tables})"

        connection = self.get_connection_object()

        table_schema_query = f'''
        SELECT TABLE_SCHEMA AS "DATABASENAME",TABLE_NAME AS "TABLENAME" FROM INFORMATION_SCHEMA.TABLES WHERE 
        TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA') {schema_query} {tables_query}'''

        # query to fetch the primary keys
        schema_query = "" if schema == "" else f'and "schema_name" in ({schema})'
        tables_query = "" if tables == "" else f'and "table_name" in ({tables})'

        tables_primary_keys = f"SHOW PRIMARY KEYS IN DATABASE {self.database};"
        extract_primary_keys = f"""
        SELECT "schema_name" as DATABASENAME,
        "table_name" as TABLENAME,
        "constraint_name",
        LISTAGG("column_name", ', ')  WITHIN GROUP (order by "key_sequence")  AS PROBABLE_NATURAL_KEY_COLUMNS
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "schema_name" not in ('INFORMATION_SCHEMA') {schema_query} {tables_query}
        group by "schema_name","table_name","constraint_name";
        """

        schema_query = "" if schema == "" else f"and tab.TABLE_SCHEMA in ({schema})"
        tables_query = "" if tables == "" else f"and tab.TABLE_NAME in ({tables})"
        view_query = " or tab.TABLE_TYPE='VIEW'" if not self.skip_views else ""

        split_by_keys = f"""
        Select tab.TABLE_SCHEMA as DATABASENAME, tab.TABLE_NAME as TABLENAME,
        LISTAGG(col.COLUMN_NAME, ',') WITHIN GROUP(ORDER BY col.ORDINAL_POSITION) as SPLIT_BY_KEY_CANDIDATES 
        from information_schema.tables tab
        INNER JOIN information_schema.columns col 
        on col.TABLE_SCHEMA = tab.TABLE_SCHEMA and col.TABLE_NAME = tab.TABLE_NAME
        WHERE (tab.TABLE_TYPE='BASE TABLE' {view_query}) AND (col.DATA_TYPE IN ('DATE','NUMBER','FLOAT') OR col.DATA_TYPE LIKE 'TIMESTAMP%')
        {schema_query} {tables_query}
        group by tab.TABLE_SCHEMA,tab.TABLE_NAME 
        order by tab.TABLE_SCHEMA,tab.TABLE_NAME;"""

        pd_table_size = pd.read_sql(table_schema_query, connection)

        cursor = connection.cursor()
        cursor.execute(tables_primary_keys)
        cursor.close()
        pd_primary_keys = pd.read_sql(extract_primary_keys, connection)

        pd_split_by_keys = pd.read_sql(split_by_keys, connection)

        pd_primary_keys = pd_primary_keys.apply(lambda x: panda_strip(x))
        pd_table_size = pd_table_size.apply(lambda x: panda_strip(x))
        pd_split_by_keys = pd_split_by_keys.apply(lambda x: panda_strip(x))

        print("pd_table_size\n", pd_table_size.info(verbose=True))
        print("pd_primary_keys\n", pd_primary_keys.info(verbose=True))
        print("pd_split_by_keys\n", pd_split_by_keys.info(verbose=True))

        resultant_temp = pd_table_size.merge(
            pd_primary_keys[['DATABASENAME', 'TABLENAME', "PROBABLE_NATURAL_KEY_COLUMNS"]],
            how="left")
        resultant = resultant_temp.merge(pd_split_by_keys[['DATABASENAME', 'TABLENAME', 'SPLIT_BY_KEY_CANDIDATES']],
                                         how="left")

        resultant = resultant.fillna('')
        return resultant


def main():
    parser = argparse.ArgumentParser(description="RDBMS Source parser", add_help=True)
    parser.add_argument('--configuration_file_path', type=str, required=False,
                        default=f"{cwd}/conf/configurations.json",
                        help='Pass the absolute path to configuration file json.Default("./conf/configurations.json")')
    subparsers = parser.add_subparsers(title='source_type', dest='source_type')
    subparsers.required = True
    parser_teradata = subparsers.add_parser("teradata",
                                            description="Teradata metadata generator",
                                            help="Teradata metadata generator parser")
    parser_teradata.add_argument('--host', type=str, required=True, help='hostname/ip of teradata database')
    parser_teradata.add_argument('--port', type=str, default="1025", help='port of teradata database (default 1025)')
    parser_teradata.add_argument('--user', type=str, required=True, help='username for teradata database')
    parser_teradata.add_argument('--password', type=str, required=True,
                                 help="encrypted password for teradata database and should not be enclosed in b''")
    parser_teradata.add_argument('--schemas', nargs='*', default=[],
                                 help='Specify the schema(s) to pull data from(comma seperated for multiple schemas)')
    parser_teradata.add_argument('--metadata_csv_path', type=str, required=False,
                                 default=f"{cwd}/csv/teradata_metadata.csv",
                                 help='Pass the absolute path to metadata csv file that will be generated.Default('
                                      '"./csv/teradata_metadata.csv")')
    parser_oracle = subparsers.add_parser("oracle",
                                          description="Oracle metadata generator",
                                          help="Oracle metadata generator parser")
    parser_oracle.add_argument('--host', type=str, required=True, help='hostname/ip of oracle database')
    parser_oracle.add_argument('--port', type=str, required=False, default='1521', help='port of oracle database')
    parser_oracle.add_argument('--service', type=str, required=False, default="xe",
                               help='service of oracle database(Eg:xe]')
    parser_oracle.add_argument('--user', type=str, required=True, help='username for oracle database')
    parser_oracle.add_argument('--password', type=str, required=True, help='password for oracle database')
    parser_oracle.add_argument('--schemas', nargs='*', default=[],
                               help='Specify the schema to pull data from(comma seperated for multiple schemas)')
    parser_oracle.add_argument('--metadata_csv_path', type=str, required=False,
                               default=f"{cwd}/csv/oracle_metadata.csv",
                               help='Pass the absolute path to metadata csv file that will be generated.Default('
                                    '"./csv/oracle_metadata.csv")')

    parser_vertica = subparsers.add_parser("vertica",
                                           description="Vertica metadata generator",
                                           help="Vertica metadata generator parser")
    parser_vertica.add_argument('--host', type=str, required=True, help='hostname/ip of Vertica database')
    parser_vertica.add_argument('--port', type=str, required=False, default='5433', help='port of Vertica database')
    parser_vertica.add_argument('--user', type=str, required=True, help='username for Vertica database')
    parser_vertica.add_argument('--password', type=str, required=True, help='password for Vertica database')
    parser_vertica.add_argument('--schemas', nargs='*', default=[],
                                help='Specify the schema to pull data from(comma seperated for multiple schemas)')
    parser_vertica.add_argument('--metadata_csv_path', type=str, required=False,
                                default=f"{cwd}/csv/vertica_metadata.csv",
                                help='Pass the absolute path to metadata csv file that will be generated.Default('
                                     '"./csv/vertica_metadata.csv")')

    parser_mysql = subparsers.add_parser("mysql",
                                         description="MYSQL metadata generator",
                                         help="MYSQL metadata generator parser")
    parser_mysql.add_argument('--host', type=str, required=True, help='hostname/ip of MYSQL database')
    parser_mysql.add_argument('--port', type=str, required=False, default='3306',
                              help='port of MYSQL database(default 3306)')
    parser_mysql.add_argument('--user', type=str, required=True, help='username for MYSQL database')
    parser_mysql.add_argument('--password', type=str, required=True, help='password for MYSQL database')
    parser_mysql.add_argument('--schemas', nargs='*', default=[],
                              help='Specify the schema to pull data from(comma seperated for multiple schemas)')
    parser_mysql.add_argument('--skip_views', type=str, choices=["True", "False"], required=False, default="False",
                              help='bool value to skip views if set to true')
    parser_mysql.add_argument('--metadata_csv_path', type=str, required=False,
                              default=f"{cwd}/csv/mysql_metadata.csv",
                              help='Pass the absolute path to metadata csv file that will be generated.Default('
                                   '"./csv/mysql_metadata.csv")')

    parser_netezza = subparsers.add_parser("netezza", description="Netezza metadata generator",
                                           help="Netezza metadata generator parser")
    parser_netezza.add_argument('--host', type=str, required=True, help='hostname/ip of Netezza database')
    parser_netezza.add_argument('--port', type=str, required=False, default="5480",
                                help='port of Netezza database(default 5480)')
    parser_netezza.add_argument('--user', type=str, required=True, help='username for Netezza database')
    parser_netezza.add_argument('--password', type=str, required=True,
                                help="encrypted password for Netezza database and should not be enclosed in b''")
    parser_netezza.add_argument('--db', type=str, required=True, help='database name in Netezza')
    parser_netezza.add_argument('--driver_path', type=str, default=[],
                                help='Specify the absolute driver JDBC jar file path along with filename')
    parser_netezza.add_argument('--schemas', nargs='*', default=[], help='Specify the schema to pull data from')
    parser_netezza.add_argument('--metadata_csv_path', type=str, required=False,
                                default=f"{cwd}/csv/netezza_metadata.csv",
                                help='Pass the absolute path to metadata csv file that will be generated.Default('
                                     '"./csv/netezza_metadata.csv")')

    parser_snowflake = subparsers.add_parser("snowflake", description="Snowflake metadata generator",
                                             help="Snowflake metadata generator parser")
    parser_snowflake.add_argument('--account_name', type=str, required=True, help='Account name')
    parser_snowflake.add_argument('--user', type=str, required=True, help='username for snowflake database')
    parser_snowflake.add_argument('--password', type=str, required=True,
                                  help="encrypted password for snowflake database and should not be enclosed in b''")
    parser_snowflake.add_argument('--warehouse', type=str, help='Specify the warehouse to execute the query on')
    parser_snowflake.add_argument('--database', type=str, help='Specify the database to pull data from')
    parser_snowflake.add_argument('--schemas', nargs='*', default=[],
                                  help='Specify the schema to pull data from (space separated)')
    parser_snowflake.add_argument('--tables', nargs='*', default=[],
                                  help='Specify the tables to pull (space separated)')
    parser_snowflake.add_argument('--metadata_csv_path', type=str, required=False,
                                  default=f"{cwd}/csv/snowflake_metadata.csv",
                                  help='Pass the absolute path to metadata csv file.Default('
                                       '"./csv/teradata_metadata.csv")')
    parser_snowflake.add_argument('--skip_views', type=str, choices=["True", "False"], required=False, default="False",
                                  help='bool value to skip views if set to true')

    args, unknown_args = parser.parse_known_args()
    print(args)
    if not check_file_writable(args.metadata_csv_path):
        print("Pass the right metadata_csv_path including file_name.Exiting..")
        exit(-100)
    if not os.path.exists(args.configuration_file_path) or not os.path.isfile(args.configuration_file_path):
        print("Either the configuration_file_path doesn't exist or is not a file.Exiting..")
        exit(-100)
    if len(unknown_args) > 0:
        print(f'Unrecognized arguments: {unknown_args}')
        exit(-100)
    rdbms_client = None
    if args.source_type == "teradata":
        rdbms_client = TeradataSource(args.host, args.port, args.user, args.password, args.schemas)
    elif args.source_type == "oracle":
        rdbms_client = OracleSource(args.host, args.port, args.user, args.password, args.schemas, args.service)
    elif args.source_type == "vertica":
        rdbms_client = VerticaSource(args.host, args.port, args.user, args.password, args.schemas)
    elif args.source_type == "mysql":
        rdbms_client = MySQLSource(args.host, args.port, args.user, args.password, args.schemas, args.skip_views)
    elif args.source_type == "netezza":
        rdbms_client = NetezzaSource(args.host, args.port, args.user, args.password, args.db, args.schemas,
                                     args.driver_path)
    elif args.source_type == "snowflake":
        rdbms_client = SnowflakeSource(args.account_name, args.user, args.password, args.warehouse, args.database,
                                       args.schemas, args.tables, args.skip_views)
    else:
        print('Either source type specified or Source type provided is not supported.')
        exit(-100)
    rdbms_client.install_and_download_dependencies()
    df = rdbms_client.generate_sql_and_get_results()
    status = assign_defaults_and_export_to_csv(df, args.source_type, args.configuration_file_path,
                                               args.metadata_csv_path)
    if status:
        print("Done")
    else:
        print("Script Failed!")


if __name__ == '__main__':
    main()
