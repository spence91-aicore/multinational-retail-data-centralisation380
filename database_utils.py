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

    def read_db_creds(self) -> None:
        """read db_creds.yaml file"""
        data_loaded : Dict
        with open('db_creds.yaml','r') as stream:
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


    def init_db_engine(self,TYPE='postgresql') -> None:
        """read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine."""
        # engine = sqlalchemy.create_engine
        if TYPE == 'sqlite':
             self._init_sqlite_engine()
             return
        DATABASE_TYPE = TYPE #'postgresql'
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

def test_upload_todb_products_data():
    from data_cleaning import DataCleaning
    from data_extraction import DataExtractor     
    db_sqlite = DatabaseConnector()
    db_sqlite.database = 'sales_data.db'
    db_sqlite.init_db_engine(TYPE='sqlite') 

    s3_uri : str = 's3://data-handling-public/products.csv'
    products_data : pd.DataFrame = DataExtractor.extract_from_s3(s3_uri)
    products_data = DataCleaning.clean_products_data(products_data=products_data)
    products_data = DataCleaning.convert_product_weights(products_data=products_data)

    db_sqlite.upload_to_db(table_name='dim_products',data_to_upload=products_data)
    a = pd.read_sql_table('dim_products',db_sqlite.engine.connect())
    print(a.head(10)) 

def test_upload_to_db_store_data():
    import requests
    from data_cleaning import DataCleaning
    from data_extraction import DataExtractor
    
    headers_dict : requests.structures.CaseInsensitiveDict = requests.structures.CaseInsensitiveDict()
    headers_dict['x-api-key'] = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    number_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    details_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    db_sqlite = DatabaseConnector()
    db_sqlite.database = 'sales_data.db'
    db_sqlite.init_db_engine(TYPE='sqlite') 
    stores_detail : pd.DataFrame

    print('fetch no:stores')
    number_of_stores = DataExtractor.list_number_of_stores(number_endpoint,headers_dict)
    print('fetching store details')
    stores_detail = DataExtractor.retrieve_stores_data(number_of_stores,details_endpoint,headers_dict)
    print('cleaning data')
    stores_detail = DataCleaning.clean_store_data(stores_detail)
    print('uploading to db')
    db_sqlite.upload_to_db(table_name='dim_store_details',data_to_upload=stores_detail)
    a = pd.read_sql_table('dim_store_details',db_sqlite.engine.connect())
    print(a.head(10))        

def test_upload_to_db_card_details():
    from data_cleaning import DataCleaning
    from data_extraction import DataExtractor
    pdf = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    db_sqlite = DatabaseConnector()
    db_sqlite.database = 'sales_data.db'
    db_sqlite.init_db_engine(TYPE='sqlite')
    a = DataCleaning()
    b = DataExtractor()
    df = b.retrieve_pdf_data(pdf)
    df = a.clean_card_data(df)
    db_sqlite.upload_to_db(table_name='dim_card_details',data_to_upload=df)
    print(db_sqlite.list_db_tables())
    a = pd.read_sql_table('dim_card_details',db_sqlite.engine.connect())
    print(a.head(10))    

def test_upload_to_db():
    from data_cleaning import DataCleaning
    df : pd.DataFrame = pd.read_pickle('legacy_user.pkl')
    a = DataCleaning()
    df = a.clean_user_data(df)
    db_sqlite = DatabaseConnector()
    db_sqlite.database = 'sales_data.db'
    db_sqlite.init_db_engine(TYPE='sqlite')
    # df.info()
    db_sqlite.upload_to_db(table_name='dim_users',data_to_upload=df) # upload it
    print(db_sqlite.list_db_tables())
    a = pd.read_sql_table('dim_users',db_sqlite.engine.connect())
    print(a.head(10))



def test_init_db_engine():
        db_connector = DatabaseConnector()
        db_connector.read_db_creds()
        db_connector.init_db_engine()
        print(db_connector.engine)
        print(db_connector.list_db_tables())
        # inspector = sqlalchemy.inspect(db_connector.engine)
        # inspector.get_table_names()
        # print(inspector.get_table_names())
        #['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']

def test_db_connector():
        db_connector = DatabaseConnector()
        db_connector.read_db_creds()
        print(db_connector.database)
        print(db_connector.host)
        print(db_connector.password)
        print(db_connector.user)
        print(db_connector.port)


if __name__ == '__main__':
    #test_db_connector()
    # test_init_db_engine()
    # test_upload_to_db()
    # test_upload_to_db_card_details()
    # test_upload_to_db_store_data()
    test_upload_todb_products_data()    
