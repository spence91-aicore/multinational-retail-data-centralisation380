import yaml
import psycopg2
import sqlite3
import pandas as pd
import sqlalchemy
from typing import Dict,List, Literal



class DatabaseConnector:
    """use to connect with and upload data to the database.
    """

    database : str
    host : str
    password : str 
    user : str 
    port : str

    engine : sqlalchemy.Engine

    _yaml_mapping : List[str] = ['RDS_HOST', 'RDS_PASSWORD', 'RDS_USER', 'RDS_DATABASE', 'RDS_PORT']

    def read_db_creds(self,filename='db_creds.yaml') -> None:
        """reads a .yaml with database credentials in it.
        if no filename param set, will default to "db_creds.yaml", in the working directory.

        yml file must be in the following format:

        RDS_HOST: (enter detail here)
        RDS_PASSWORD: (enter detail here)
        RDS_USER: (enter detail here)
        RDS_DATABASE: (enter detail here)
        RDS_PORT: (enter detail here)

        """
        data_loaded : Dict
        with open(filename,'r') as stream:
        #yaml.reader('db')
            data_loaded = yaml.safe_load(stream) # dict
            self.database = data_loaded['RDS_DATABASE']
            self.host = data_loaded['RDS_HOST']
            self.password = data_loaded['RDS_PASSWORD']
            self.user = data_loaded['RDS_USER']
            self.port = data_loaded['RDS_PORT']


    def _init_sqlite_engine(self) -> None:
        """this is used by init_db_engine, to init a sql instance instead of postgres"""
        def create_sqlite_database(filename):
            """create a database if one doesn't exist"""
            conn = None
            try:
                conn = sqlite3.connect(filename)
                print(sqlite3.sqlite_version)
            except sqlite3.Error as e:
                print(e)
            finally:
                if conn:
                    conn.close()
        create_sqlite_database(self.database)
        DATABASE_TYPE = 'sqlite'
        DATABASE = self.database
        engine = sqlalchemy.create_engine(f"{DATABASE_TYPE}:///{DATABASE}")
        engine.connect()
        self.engine = engine


    def init_db_engine(self,db_type : Literal['postgresql','sqlite'] = 'postgresql') -> None:
        """read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine."""
        # engine = sqlalchemy.create_engine
        if db_type == 'sqlite':
             self._init_sqlite_engine()
             return
        DATABASE_TYPE = db_type #'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = self.host
        USER = self.user
        PASSWORD = self.password
        PORT = 5432
        DATABASE = self.database

        engine = sqlalchemy.create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        engine.connect()
        self.engine = engine

    def list_db_tables(self) -> List:
         """Returns a list of tables in list format"""
         table_names : List
         table_names = sqlalchemy.inspect(self.engine).get_table_names()
         return table_names
    
    def upload_to_db(self,table_name : str, data_to_upload : pd.DataFrame, if_exists: Literal['fail', 'replace', 'append']='replace'):
         """uses out-of-the box pandas fun"""
         data_to_upload.to_sql(table_name,self.engine,if_exists=if_exists)

