import json
import requests
import requests.structures
from database_utils import DatabaseConnector
import pandas as pd
import tabula
from typing import List, Any


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
            responce = requests.Response = requests.get(url=url,headers=headers_dict)
            if responce.status_code == 200:
                store_list.append(json.loads(responce.text))
            else:
                raise NameError('unable to fetch data %s, %s' % (responce.status_code, responce.text))

        #return dataframe, converting the list
        return pd.DataFrame(store_list)

     

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



def test_databaseconnector():
    a = DataExtractor()
    a.read_rds_table('legacy_store_details')
    # print(a.databaseconnector.list_db_tables())


def test_bad_databaseconnector():
    a = DataExtractor()
    a.read_rds_table('thing')
    print(a.databaseconnector.list_db_tables())

def test_tabula():
    import tabula
    import pandas as pd
    pdf = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    # pdf = 'card_details.pdf'
    df = tabula.read_pdf(pdf,stream=True)

if __name__ == '__main__':
    #test_db_connector()
    # test_init_db_engine()
    # test_databaseconnector()
    # test_tabula()
    # test_list_number_of_stores()
    test_retireve_stores_data()