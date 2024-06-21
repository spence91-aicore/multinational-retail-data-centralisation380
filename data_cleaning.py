import pandas as pd
import numpy
from dateutil.parser import parse
from dateutil.parser import ParserError
import datetime
from typing import List

class DataCleaning:
    """methods to clean data from each of the data sources"""

    @staticmethod
    def _tidy_dates(date_col:pd.DataFrame) -> pd.DataFrame:
        @staticmethod
        def process_dates(input:str) -> datetime.datetime:
            try:
                return parse(input)
            except ParserError as e:
                # Handle insane dates by putting in a None
                if 'Unknown string format' in e.__str__():
                    return None
                if 'ParserError: day is out of range for ' in e.__str__():
                    return None

        # if it's already a datetime, do nothing, and return what's already there
        if pd.api.types.is_datetime64_any_dtype(date_col):
            return date_col                   
        return pd.to_datetime(date_col.apply(process_dates))
    
    @staticmethod
    def _country_code_convert_ggb(row: pd.Series) -> str:
        """helper function for cleaning user data:
        if country code is GGB, and country is UK, return GB """
        # df['country_code'] = df.apply(lambda row: 
        if row.loc['country'] == 'United Kingdom' and row.loc['country_code'] == 'GGB':
            return 'GB' 
        else:
            return row.loc['country_code']
        
    @staticmethod
    def _remove_rows_of_bad_data(user_data:pd.DataFrame) -> pd.DataFrame:
        """removes rows where there appears to be a 10char string in each col
        check the first 4 cols"""
        regex_for_bad_rows = '^[0-9A-Z]{10}$'
        # user_data.drop('Unnamed',axis=1,errors='ignore')
        #card_data.drop('index',axis=1,errors='ignore').columns
        match_mask : pd.Series = (user_data[user_data.drop('index',axis=1,errors='ignore').columns[0]].astype('str').str.match(regex_for_bad_rows) & \
                user_data[user_data.drop('index',axis=1,errors='ignore').columns[1]].astype('str').str.match(regex_for_bad_rows) & \
                user_data[user_data.drop('index',axis=1,errors='ignore').columns[2]].astype('str').str.match(regex_for_bad_rows) & \
                user_data[user_data.drop('index',axis=1,errors='ignore').columns[3]].astype('str').str.match(regex_for_bad_rows))
        return user_data.drop(user_data[match_mask].index)

    @staticmethod
    def _remove_rows_of_nan_data(user_data:pd.DataFrame) -> pd.DataFrame:
        """removes rows where  """

        user_data

    @staticmethod
    def _remove_rows_of_null_data(user_data:pd.DataFrame) -> pd.DataFrame:
        """removes rows where the second col  are 'NULL', assumption is that
        if there is not any decent data in the first two rows, there isn't in the rest """
        match_mask : pd.Series = ( user_data[user_data.drop('index',axis=1,errors='ignore').columns[0]] == 'NULL' ) & \
                                 ( user_data[user_data.drop('index',axis=1,errors='ignore').columns[1]] == 'NULL')
                                #   & \
                                    # (user_data[user_data['last_name'] == 'NULL']   ) )
        return user_data.drop(user_data[match_mask].index)

    @staticmethod
    def _cleanse_phone_data(row: pd.Series) -> str:
        """helper function for cleaning phone data:
            Cleans in different ways depending on 'country_code value:'
            if US:
                remove parenthesis, dashes, dots, and make sure the international code is there
            if DE / GB:
                remove parenthesis, make sure interenational code is there
            WARNING - this uses 'country_code' - so make sure that col has been CLEANSED!
        """
        # df['country_code'] = df.apply(lambda row:
        # print(row)
        number : str = row.loc['phone_number'] 
        if row.loc['country_code'] == 'US': #and row.loc['country_code'] == 'GGB':
            # print(number)
            if number.startswith('(') and ')' in number: 
                number = number.replace('(','')
                number =  number.replace(')',' ')
            number = number.replace('.', ' ')
            number = number.replace('-',' ')
            number = number.replace('x', ' ext ' )
            if number.startswith('001'):
                number = number[3:]
                number = number.strip()
            if not number.startswith('+1'):
                number = '+1 ' + number
            # return 'US'
            return number
        # return 'GB'     
        if row.loc['country_code'] == 'GB':
            if number.startswith('+44') and '(0)' in number: # get rid of 
                number = number.replace('(0)','')
            if number.startswith('('): # get rid of parentheses if 
                number = number.replace('(','')
                number =  number.replace(')','')
            if number.startswith('0'): # replace leading 0 with +44 - because we're a multinational
                number = '+44' + number[1:]
            # number = number.replace(' ','')
            if number.startswith('44'): # put a plus in front of it
                number = '+' + number
            return number
        if row.loc['country_code'] == 'DE':
            if number.startswith('+49') and '(0)' in number: # get rid of 
                number = number.replace('(0)','')
            if number.startswith('('): # get rid of parentheses if 
                number = number.replace('(','')
                number =  number.replace(')','')
            if number.startswith('0'): # replace leading 0 with +44 - because we're a multinational
                number = '+49' + number[1:]
                number = number.replace(' ','')
            if number.startswith('49'): # put a plus in front of it
                number = '+' + number                
            return number
        else:
            # print(row.loc['phone_number'])
            return row.loc['phone_number']    

    @staticmethod
    def _df_col_sanity_check(expected_cols:List[str], df:pd.DataFrame) -> None:
        """throws error if col doesn't exist, and we expect it to"""
        # for col_name in df.columns:
        for col_name in expected_cols:
            if not col_name in df.columns:
                raise ValueError('Please check data source, expected Column "%s" not there' % col_name)

    def clean_user_data(self,user_data:pd.DataFrame) -> pd.DataFrame:
        user_data = self._remove_rows_of_bad_data(user_data)
        user_data = self._remove_rows_of_null_data(user_data)
        user_data['date_of_birth'] = self._tidy_dates(user_data['date_of_birth'])
        user_data['join_date'] = self._tidy_dates(user_data['join_date'])
        user_data['country_code'] = user_data.apply(self._country_code_convert_ggb,axis=1)    
        user_data['phone_number'] = user_data.apply(self._cleanse_phone_data,axis=1) # must be run after country_code cleanse!
        return user_data


    def clean_card_data(card_data:pd.DataFrame) -> pd.DataFrame:
        # sort out cols

        #sanity check 
        expected_cols : List[str] = ['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed']
        DataCleaning._df_col_sanity_check(expected_cols,card_data)

        # sort out dodgy col, split by first whitespace
        col_to_split = 'card_number expiry_date'
        card_no_not_nulls = card_data[card_data[col_to_split].notnull() ]

        # split into two cols, by the whitespace
        split_pd : pd.DataFrame= card_no_not_nulls['card_number expiry_date'].str.split(" ", n=1, expand=True)

        # insert processed rows into the right place
        card_data.loc[split_pd.index, ['card_number']] = split_pd[0]
        card_data.loc[split_pd.index, ['expiry_date']] = split_pd[1]
     
        # drop col
        card_data = card_data.drop([col_to_split],axis=1)

        # remove other null cols (that might have been erronneusly created by the pdf reader)
        card_data = card_data.dropna(axis=1,how='all')

        card_data = DataCleaning._remove_rows_of_bad_data(card_data)
        card_data = DataCleaning._remove_rows_of_null_data(card_data)
        # card_data['expiry_date'] = self._tidy_dates(card_data['expiry_date'])
  
        #convert to date
        card_data['date_payment_confirmed'] = DataCleaning._tidy_dates(card_data['date_payment_confirmed'])

        # remove weirdness from card_number
        card_data['card_number'] = card_data['card_number'].astype('str').apply(lambda x : x.strip('?'))
    
        # convert col to int
        card_data['card_number'] = pd.to_numeric(card_data['card_number'],downcast='integer')

        return card_data

    @staticmethod
    def clean_store_data(store_data : pd.DataFrame) -> pd.DataFrame:

        # remove rows of "bad data"
        store_data = DataCleaning._remove_rows_of_bad_data(store_data)

        #remove null rows
        store_data = DataCleaning._remove_rows_of_null_data(store_data)

        # make sure store types that are of type "web portal" don't have lat/long - doesn't make sense
        web_portal = store_data[store_data['store_type'] == 'Web Portal']
        store_data.loc[web_portal.index,['lattitude']] = numpy.nan
        store_data.loc[web_portal.index,['longitude']] = numpy.nan

        # clean up "continent" values where there is an 'ee' at the start:
        ee_fix = store_data[store_data['continent'].str.startswith('ee')]
        store_data.loc[ee_fix.index,['continent']] = ee_fix['continent'].astype('str').apply(lambda x: len(x)>2 and x[2:] or x)

        #remove newlines from addresses:
        store_data['address'] = store_data['address'].astype('str').apply(lambda x : x.replace('\n',', '))

        # sort dates 
        store_data['opening_date'] = DataCleaning._tidy_dates(store_data['opening_date'])

        # drop cols that aren't used
        store_data = store_data.dropna(axis='columns',thresh=5)

        # sanitise staff numbers
        store_data['staff_numbers'] = store_data['staff_numbers'].astype('str').str.replace('[^0-9]+','',regex=True)

        # change staff number to int
        store_data['staff_numbers'] = store_data['staff_numbers'].astype('int',errors='ignore')

        # change lat / long to float
        store_data['latitude'] = store_data['latitude'].astype('float')  
        store_data['longitude'] = store_data['longitude'].astype('float')  

        return store_data
    
    @staticmethod
    def clean_products_data(products_data: pd.DataFrame) -> pd.DataFrame:
        products_data = products_data.drop('Unnamed: 0',axis=1,errors='ignore') # YOU NEED TO SORT THIS OUT - don't ignore this!!
        products_data = DataCleaning._remove_rows_of_bad_data(products_data)
        products_data = DataCleaning._remove_rows_of_null_data(products_data)
        products_data = products_data.dropna(axis=0,how='all')

        # convert to datetime
        products_data['date_added'] = DataCleaning._tidy_dates(products_data['date_added'])
        

        return products_data

    @staticmethod
    def convert_product_weights(products_data: pd.DataFrame) -> pd.DataFrame:
        """STRONGLY RECOMEND putting dataframe through clean_products_data() first
        tries to normalise weights to .kg
        """
        def split_and_and_multiply(i :str) -> float:
            val_list = i.split(' x ')
            # print(val_list)
            return float(val_list[0]) * float(val_list[1])
    
        # remove spaces and stops from the end of the string
        products_data['weight'] = products_data['weight'].astype('str').str.replace('[ .]+$','',regex=True)

        ### find all things that end with kg
        kg_df = products_data['weight'].astype('str').str.endswith('kg')
        # products_data.loc[kg_df,['weight_type']] = 'kg'
        products_data.loc[kg_df,['weight']] = products_data[kg_df]['weight'].astype('str').str.replace('kg','',regex=False) #.astype('float')

        ### find all things that end with g
        gram_df = products_data['weight'].astype('str').str.endswith('g')
        # products_data.loc[gram_df,['weight_type']] = 'g'
        # 11 and 12 index - are 1.2 grams - that's clearly not right - fix?
        products_data.loc[gram_df,['weight']] = products_data[gram_df]['weight'].astype('str').str.replace('g','',regex=False) #.astype('float')

        # find all things that end ml
        ml_df = products_data['weight'].astype('str').str.endswith('ml')
        # products_data.loc[ml_df,['weight_type']] = 'ml'
        products_data.loc[ml_df,['weight']] = products_data[ml_df]['weight'].astype('str').str.replace('ml','',regex=False) #astype('float')

        # find all things that end oz
        oz_df = products_data['weight'].astype('str').str.endswith('oz')
        # products_data.loc[oz_df,['weight_type']] = 'oz'
        products_data.loc[oz_df,['weight']] = products_data[oz_df]['weight'].astype('str').str.replace('oz','',regex=False)

        # find all things that have a ' x ' in it, times it together
        times_df = products_data['weight'].astype('str').str.contains(' x ')
        products_data.loc[times_df,['weight']] = products_data[times_df]['weight'].apply(split_and_and_multiply)

        ##conversions to kg
        # convert weight to float type
        products_data['weight'] = products_data['weight'].astype('float')
        # conversion on ml to kg
        products_data.loc[ml_df,['weight']] = products_data[ml_df]['weight'].apply(lambda x : x / 1000)
        # conversion on g to kg
        products_data.loc[gram_df,['weight']] = products_data[gram_df]['weight'].apply(lambda x : x / 1000)
        # conversion of oz to kg = 0.0283
        products_data.loc[oz_df,['weight']] = products_data[oz_df]['weight'].apply(lambda x: x * 0.0283)
        return products_data
    
    @staticmethod
    def clean_orders_data(orders_data:pd.DataFrame) -> pd.DataFrame:
        """expect orders data, drops first name, last name, 1, and level_0 col, if they exist"""
        orders_data = orders_data.drop('first_name',axis=1,errors='ignore')
        orders_data = orders_data.drop('last_name',axis=1,errors='ignore')
        orders_data = orders_data.drop('1',axis=1,errors='ignore')
        orders_data = orders_data.drop('level_0', axis=1, errors='ignore')
        orders_data = orders_data.reset_index(drop=True)
        return orders_data        

    @staticmethod
    def clean_datetimes_data(datetimes_data:pd.DataFrame) -> pd.DataFrame:
        "expect datetimes, cleans data, changes types for ints, adds a new datetime col"

        #remove bad lines
        datetimes_data = DataCleaning._remove_rows_of_bad_data(datetimes_data)
        datetimes_data = DataCleaning._remove_rows_of_null_data(datetimes_data)

        #convert types
        datetimes_data['month'] = datetimes_data['month'].astype('int')
        datetimes_data['day'] = datetimes_data['day'].astype('int')
        datetimes_data['year'] = datetimes_data['year'].astype('int')

        # new datetime col
        datetimes_data['datetime'] = pd.to_datetime( \
                    datetimes_data.apply(lambda x: "%s-%s-%s-%s" % (x['year'],x['month'],x['day'],x['timestamp']), axis=1) \
                    ,format='%Y-%m-%d-%H:%M:%S')
        
        return datetimes_data


def test_clean_datetimes_data():
    df : pd.DataFrame = pd.read_pickle('timestamps.pkl')
    df = DataCleaning.clean_datetimes_data(df)
    print(df)


def test_convert_product_weights():
    df : pd.DataFrame = pd.read_pickle('s3_data.pkl')
    df = DataCleaning.clean_products_data(df)
    df = DataCleaning.convert_product_weights(df) 
    print(df)   

def test_clean_user_data():
    products_data : pd.DataFrame = pd.read_pickle('legacy_user.pkl')
    a = DataCleaning()
    df = a.clean_user_data(df)
    df.info()
    df.to_clipboard()

def test_clean_card_data():
    card_data: pd.DataFrame = pd.read_pickle('pdf_data.pkl')
    # original_card_data = pd.read_pickle('pdf_data.pkl')
    from data_cleaning import DataCleaning
    dc = DataCleaning
    card_data['card_number'].describe()
    card_data = dc.clean_card_data(card_data)
    print(card_data)

def test_clean_store_data():
    store_data: pd.DataFrame = pd.read_pickle('api_data.pkl')
    a = DataCleaning.clean_store_data(store_data=store_data)
    print(a.info())

if __name__ == '__main__':
    # test_clean_user_data()
    # test_clean_card_data()
    # test_clean_store_data()
    # test_convert_product_weights()
    test_clean_datetimes_data()
# df = a.read_rds_table('legacy_users')
# df.to_pickle('legacy_user.pkl')
