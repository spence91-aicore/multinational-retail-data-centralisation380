import pandas as pd
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

local_db_yaml = 'db_creds_local_postgres.yaml'

def test_upload_todb_datetimes_data():
    from data_cleaning import DataCleaning
    from data_extraction import DataExtractor 
    db_sqlite = DatabaseConnector()
    # db_sqlite.database = 'sales_data.db'
    # db_sqlite.read_db_creds('sqlite_creds.yaml')  
    # db_sqlite.init_db_engine(db_type='sqlite')    
    db_sqlite.read_db_creds(local_db_yaml)
    db_sqlite.init_db_engine()
    http_uri = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    datetimes_data = pd.DataFrame = DataExtractor.extract_json_from_http(http_uri)
    datetimes_data = DataCleaning.clean_datetimes_data(datetimes_data)
    db_sqlite.upload_to_db(table_name='dim_date_times',data_to_upload=datetimes_data,if_exists='replace')
    # a = pd.read_sql_table('dim_date_times',db_sqlite.engine.connect())
    # print(a.head(10))

def test_upload_todb_orders_data():
    from data_cleaning import DataCleaning
    from data_extraction import DataExtractor 
    #db_sqlite = DatabaseConnector()
    #db_sqlite.database = 'sales_data.db'
    #db_sqlite.init_db_engine(db_type='sqlite')     
    db_sqlite = DatabaseConnector()
    db_sqlite.read_db_creds(local_db_yaml)
    db_sqlite.init_db_engine()
    a = DataExtractor()
    orders_data : pd.DataFrame = a.read_rds_table('orders_table')    
    orders_data = DataCleaning.clean_orders_data(orders_data)
    # print(b)
    db_sqlite.upload_to_db(table_name='orders_table',data_to_upload=orders_data,if_exists='replace')
    a = pd.read_sql_table('orders_table',db_sqlite.engine.connect())
    print(a.head(10))


def test_upload_todb_products_data():
    from data_cleaning import DataCleaning
    from data_extraction import DataExtractor     
    #db_sqlite = DatabaseConnector()
    #db_sqlite.database = 'sales_data.db'
    #db_sqlite.init_db_engine(db_type='sqlite') 
    db_sqlite = DatabaseConnector()
    db_sqlite.read_db_creds(local_db_yaml)
    db_sqlite.init_db_engine()

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
    #db_sqlite = DatabaseConnector()
    #db_sqlite.database = 'sales_data.db'
    #db_sqlite.init_db_engine(db_type='sqlite') 
    db_sqlite = DatabaseConnector()
    db_sqlite.read_db_creds(local_db_yaml)
    db_sqlite.init_db_engine()
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
    #db_sqlite = DatabaseConnector()
    #db_sqlite.database = 'sales_data.db'
    #db_sqlite.init_db_engine(db_type='sqlite')
    db_sqlite = DatabaseConnector()
    db_sqlite.read_db_creds(local_db_yaml)
    db_sqlite.init_db_engine()
    # a = DataCleaning()
    b = DataExtractor()
    df = b.retrieve_pdf_data(pdf)
    df = DataCleaning.clean_card_data(df)
    db_sqlite.upload_to_db(table_name='dim_card_details',data_to_upload=df)
    print(db_sqlite.list_db_tables())
    a = pd.read_sql_table('dim_card_details',db_sqlite.engine.connect())
    print(a.head(10))    

def test_upload_to_db():
    from data_cleaning import DataCleaning
    df : pd.DataFrame = pd.read_pickle('legacy_user.pkl')
    a = DataCleaning()
    df = a.clean_user_data(df)
    #db_sqlite = DatabaseConnector()
    #db_sqlite.database = 'sales_data.db'
    #db_sqlite.init_db_engine(db_type='sqlite')
    db_sqlite = DatabaseConnector()
    db_sqlite.read_db_creds(local_db_yaml)
    db_sqlite.init_db_engine()
    # df.info()
    db_sqlite.upload_to_db(table_name='dim_users',data_to_upload=df) # upload it
    print(db_sqlite.list_db_tables())
    a = pd.read_sql_table('dim_users',db_sqlite.engine.connect())
    print(a.head(10))



def test_init_db_engine():
        db_connector = DatabaseConnector()
        db_connector.read_db_creds(filename='db_creds_local_postgres.yaml')
        db_connector.init_db_engine()
        print(db_connector.engine)
        print(db_connector.list_db_tables())
        # inspector = sqlalchemy.inspect(db_connector.engine)
        # inspector.get_table_names()
        # print(inspector.get_table_names())
        #['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']

def test_db_connector():
        db_connector = DatabaseConnector()
        db_connector.read_db_creds(filename='db_creds_local_postgres.yaml')
        print(db_connector.database)
        print(db_connector.host)
        print(db_connector.password)
        print(db_connector.user)
        print(db_connector.port)

#uncomment functions below to run extraction, cleaning, and uploading to db 
if __name__ == '__main__':
    # test_db_connector()
    # test_init_db_engine()
    # test_upload_to_db()
    # test_upload_to_db_card_details()
    # test_upload_to_db_store_data()
    # test_upload_todb_products_data() 
    test_upload_todb_orders_data()   
    # test_upload_todb_datetimes_data()
