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
        #card_data.drop('index',axis=1,errors='ignore').columns
        match_mask : pd.Series = (user_data[user_data.drop('index',axis=1,errors='ignore').columns[0]].astype('str').str.match(regex_for_bad_rows) & \
                user_data[user_data.drop('index',axis=1,errors='ignore').columns[1]].astype('str').str.match(regex_for_bad_rows) & \
                user_data[user_data.drop('index',axis=1,errors='ignore').columns[2]].astype('str').str.match(regex_for_bad_rows) & \
                user_data[user_data.drop('index',axis=1,errors='ignore').columns[3]].astype('str').str.match(regex_for_bad_rows))
        return user_data.drop(user_data[match_mask].index)


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
    def clean_store_data(store_data : pd.DataFrame):

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



def test_clean_user_data():
    df : pd.DataFrame = pd.read_pickle('legacy_user.pkl')
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
    test_clean_store_data()
# df = a.read_rds_table('legacy_users')
# df.to_pickle('legacy_user.pkl')
