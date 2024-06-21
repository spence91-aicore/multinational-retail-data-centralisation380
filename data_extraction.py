import json
import io 
import botocore.response
import requests
import requests.structures
from database_utils import DatabaseConnector
import pandas as pd
import tabula
import boto3 
import botocore
from typing import List, Any, Dict


class DataExtractor:
    """This class will work as a utility class, 
    in it you will be creating methods that help extract data from different data sources.
    The methods contained will be fit to extract data from a particular data source, 
    these sources will include CSV files, an API and an S3 bucket.
    """

    databaseconnector : DatabaseConnector #= DatabaseConnector()

    def __init__(self) -> None:
        self.databaseconnector = DatabaseConnector()
        self.databaseconnector.read_db_creds()
        self.databaseconnector.init_db_engine()        
        # pass        

    def read_rds_table(self,db_table:str) -> pd.DataFrame:

        if db_table not in self.databaseconnector.list_db_tables():
            raise NameError('db table "%s" not in list of database tables, unable to read, available tables:%s' % (db_table,self.databaseconnector.list_db_tables()))
        table : pd.DataFrame = pd.read_sql_table(db_table, self.databaseconnector.engine.connect())
        # print(table.head())
        return table
    
    def retrieve_pdf_data(self,link:str) -> pd.DataFrame:
        """uses tabular to read tables in pdfs - this one fetches all pages
        It'll merge all the tables together to make one big one"""
        merged_df : pd.DataFrame = pd.DataFrame()
        df_list : List[pd.DataFrame] = tabula.read_pdf(link,stream=True,pages="all")
        # merge all the dataframes from each page, into one single dataframe:
        for df in df_list: 
            merged_df = pd.concat([merged_df,df],ignore_index=True)
        return merged_df

    @staticmethod
    def list_number_of_stores(number_stores_endpoint:str,headers_dict: requests.structures.CaseInsensitiveDict) -> int:
        """fetches the number of stores from an API endpoint, returns the number of stores as an int"""
        responce : requests.Response 
        responce = requests.get(url=number_stores_endpoint,headers=headers_dict)
        stores_key = 'number_stores'
        if responce.status_code == 200 and stores_key in responce.text:
            return json.loads(responce.text)[stores_key]
        else:
            raise NameError('unable to fetch data %s, %s' % (responce.status_code,responce.text))
    
    @staticmethod
    def retrieve_stores_data(num_stores:int,retrieve_store_endpoint:str,headers_dict:requests.structures.CaseInsensitiveDict) -> pd.DataFrame:
        """retieves store data"""
        # check endpoint
        store_range:range = range(0,num_stores)
        store_list : List[Any] = [] # put store data in here

        if not retrieve_store_endpoint.endswith('/'):
            retrieve_store_endpoint = retrieve_store_endpoint + '/'

        #iterate and capture each store detail, 0-n
        for store_num in store_range:
            url : str = retrieve_store_endpoint + str(store_num)
            print('fetching %s' % url)
            responce : requests.Response = requests.get(url=url,headers=headers_dict)
            if responce.status_code == 200:
                store_list.append(json.loads(responce.text))
            else:
                raise NameError('unable to fetch data %s, %s' % (responce.status_code, responce.text))

        #return dataframe, converting the list
        return pd.DataFrame(store_list)
    
    @staticmethod
    def extract_from_s3(s3_address:str) -> pd.DataFrame:
        """fetches data from s3 MUST BE CSV FORMAT
        MUST ALREADY BE AUTHENTICED, otherswise errors"""
        s3  = boto3.client('s3')
        bucket, key = s3_address.split('/',2)[-1].split('/',1)
        csv_object : Dict[Any] = s3.get_object(Bucket=bucket, Key=key)
        s3_df : pd.DataFrame
        if (    isinstance(csv_object,dict) # confirm we got a dict back
                and 'Body' in csv_object.keys() #confirm we have the thing we want to DL
                and isinstance(csv_object['Body'],botocore.response.StreamingBody) ): #confirm it's what we want
            bytes_object : bytes = csv_object['Body'].read()
            s3_df = pd.read_csv(io.BytesIO(bytes_object))   
        else: 
            raise ValueError('unable to DL from S3, please check URI')

        return s3_df
    
    def extract_json_from_http(http_uri:str) -> pd.DataFrame:
        """fetches data, from http sources
        DATA MUST BE IN JSON FORMAT!
        e.g http_uri='https://data-handling.com/date_details.json
        """
        
        responce : requests.Response = requests.get(http_uri)
        # pd.read_json
        return pd.read_json(responce.text)


def test_get_json_from_http():
    http_uri = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    b = pd.DataFrame = DataExtractor.extract_json_from_http(http_uri)
    print(b)
    b.to_pickle('timestamps.pkl')
     
def test_get_s3_date():
    s3_uri : str = 's3://data-handling-public/products.csv'
    b : pd.DataFrame = DataExtractor.extract_from_s3(s3_uri)
    print(b)
    b.to_pickle('s3_data.pkl')

def test_retireve_stores_data():
    a = DataExtractor()
    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    headers_dict : requests.structures.CaseInsensitiveDict = requests.structures.CaseInsensitiveDict()
    # headers_dict["Domain"] = self.IplicitDomain
    # headers_dict["Content-Type"] = "application/json"
    headers_dict['x-api-key'] = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    b : pd.DataFrame = a.retrieve_stores_data(retrieve_store_endpoint=url,headers_dict=headers_dict,num_stores=451)
    print(b)    
    b.to_pickle('api_data.pkl')

def test_list_number_of_stores():
    a = DataExtractor()
    url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    headers_dict : requests.structures.CaseInsensitiveDict = requests.structures.CaseInsensitiveDict()
    # headers_dict["Domain"] = self.IplicitDomain
    # headers_dict["Content-Type"] = "application/json"
    headers_dict['x-api-key'] = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    b = a.list_number_of_stores(number_stores_endpoint=url,headers_dict=headers_dict)
    print(b)

def test_get_orders_table_data():
    a = DataExtractor()
    b : pd.DataFrame = a.read_rds_table('orders_table')
    b.to_pickle('orders_data.pkl')

def test_databaseconnector():
    a = DataExtractor()
    # a.read_rds_table('legacy_store_details')
    print(a.databaseconnector.list_db_tables())


def test_bad_databaseconnector():
    a = DataExtractor()
    # a.read_rds_table('thing')
    print(a.databaseconnector.list_db_tables())

def test_tabula():
    import tabula
    import pandas as pd
    pdf = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    # pdf = 'card_details.pdf'
    df = tabula.read_pdf(pdf,stream=True)

# def test_

if __name__ == '__main__':
    # test_get_orders_table_data()
    #test_db_connector()
    # test_init_db_engine()
    # test_databaseconnector()
    # test_tabula()
    # test_list_number_of_stores()
    # test_retireve_stores_data()
    # test_get_s3_date()
    test_get_json_from_http()