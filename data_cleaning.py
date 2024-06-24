import datetime
from typing import List
import pandas as pd
import numpy
import dateutil


class DataCleaning:
    """methods to clean data from each of the data sources"""

    ##Generic helper functions for cleaning:

    @staticmethod
    def _tidy_dates(date_col:pd.DataFrame) -> pd.DataFrame:
        """takes a col of date-like data and tries its hardest to return a formatted date
        
        if it's already a date, it'll return a date
        if it can't work out how to make the data a date, it'll return a None
        ONLY SUPPLY A SINGLE COL dataframe.      
        """
        @staticmethod
        def process_dates(input:str) -> datetime.datetime:
            try:
                return dateutil.parser.parse(input)
            except dateutil.parser.ParserError as e:
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
    def _remove_rows_of_bad_data(df:pd.DataFrame,regex_for_bad_rows='^[0-9A-Z]{10}$') -> pd.DataFrame:
        """removes rows from a dataframe where all columns contain the same regex pattern
        if regex_for_bad_rows is not passed, the default is to look for a 10-char pattern than contains digits and upper case A-Z chars.
        This routine will check the first 4 cols for this match before deciding to drop"""

        if len(df.columns) < 4:
            raise NotImplementedError('routine is not made for row where there are less than 4 columns')

        match_mask : pd.Series = (df[df.drop('index',axis=1,errors='ignore').columns[0]].astype('str').str.match(regex_for_bad_rows) & \
                df[df.drop('index',axis=1,errors='ignore').columns[1]].astype('str').str.match(regex_for_bad_rows) & \
                df[df.drop('index',axis=1,errors='ignore').columns[2]].astype('str').str.match(regex_for_bad_rows) & \
                df[df.drop('index',axis=1,errors='ignore').columns[3]].astype('str').str.match(regex_for_bad_rows))
        return df.drop(df[match_mask].index)

 
    @staticmethod
    def _remove_rows_of_null_data(df:pd.DataFrame) -> pd.DataFrame:
        """removes rows where the second col  are 'NULL', assumption is that
        if there is not any decent data in the first two rows, there isn't in the rest"""

        if len(df.columns) < 2:
            raise NotImplementedError('routine is not made for row where there are less than 4 columns')

        match_mask : pd.Series = ( df[df.drop('index',axis=1,errors='ignore').columns[0]] == 'NULL' ) & \
                                 ( df[df.drop('index',axis=1,errors='ignore').columns[1]] == 'NULL')
        return df.drop(df[match_mask].index)

    @staticmethod
    def _df_col_sanity_check(expected_cols:List[str], df:pd.DataFrame) -> None:
        """throws error if col doesn't exist, and we expect it to"""
        # for col_name in df.columns:
        for col_name in expected_cols:
            if not col_name in df.columns:
                raise ValueError('Please check data source, expected column "%s" not there' % col_name)    

    # DATA Specific helper functions here 
    @staticmethod
    def _country_code_convert_ggb(row: pd.Series) -> str:
        """helper function for cleaning user data:
        if country code is GGB, and country is UK, return GB """
        if row.loc['country'] == 'United Kingdom' and row.loc['country_code'] == 'GGB':
            return 'GB' 
        else:
            return row.loc['country_code']

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



    def clean_user_data(self,user_data:pd.DataFrame) -> pd.DataFrame:
        #remove bad data rows, remove nulls
        user_data = self._remove_rows_of_bad_data(user_data)
        user_data = self._remove_rows_of_null_data(user_data)

        # sort out dates
        user_data['date_of_birth'] = self._tidy_dates(user_data['date_of_birth'])
        user_data['join_date'] = self._tidy_dates(user_data['join_date'])

        # functions for specific transforms
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
     
        # drop col, it's not needed anymore
        card_data = card_data.drop([col_to_split],axis=1)

        # remove other null cols (that might have been erronneusly created by the pdf reader)
        card_data = card_data.dropna(axis=1,how='all')

        #remove bad data rows, remove nulls
        card_data = DataCleaning._remove_rows_of_bad_data(card_data)
        card_data = DataCleaning._remove_rows_of_null_data(card_data)
  
        #convert to date
        card_data['date_payment_confirmed'] = DataCleaning._tidy_dates(card_data['date_payment_confirmed'])

        # remove remaining weirdness from card_number
        card_data['card_number'] = card_data['card_number'].astype('str').apply(lambda x : x.strip('?'))
    
        # convert col to int
        card_data['card_number'] = pd.to_numeric(card_data['card_number'],downcast='integer')

        return card_data

    @staticmethod
    def clean_store_data(store_data : pd.DataFrame) -> pd.DataFrame:

        #remove bad data rows, remove nulls
        store_data = DataCleaning._remove_rows_of_bad_data(store_data)
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

        # sanitise staff numbers, remove anything that's not a number
        store_data['staff_numbers'] = store_data['staff_numbers'].astype('str').str.replace('[^0-9]+','',regex=True)

        # change staff number to int, you can never have half a human on your staff role
        store_data['staff_numbers'] = store_data['staff_numbers'].astype('int',errors='ignore')

        # change lat / long to float
        store_data['latitude'] = store_data['latitude'].astype('float')  
        store_data['longitude'] = store_data['longitude'].astype('float')  

        return store_data
    
    @staticmethod
    def clean_products_data(products_data: pd.DataFrame) -> pd.DataFrame:

        # remove errounious column, if it's there
        products_data = products_data.drop('Unnamed: 0',axis=1,errors='ignore') # YOU NEED TO SORT THIS OUT - don't ignore this!!

        #remove bad data rows, remove nulls
        products_data = DataCleaning._remove_rows_of_bad_data(products_data)
        products_data = DataCleaning._remove_rows_of_null_data(products_data)

        # remove other null cols
        products_data = products_data.dropna(axis=0,how='all')

        # convert to datetime
        products_data['date_added'] = DataCleaning._tidy_dates(products_data['date_added'])

        return products_data

    @staticmethod
    def convert_product_weights(products_data: pd.DataFrame,weight_col='weight') -> pd.DataFrame:
        """
        tries to normalise weights to .kg
        
        'expects' a col that has data like:

        10kg
        1000g
        10000ml

        and will replace that col with a float type, where all values have been changed to .kg format, e.g

        10
        1
        10

        STRONGLY RECOMEND putting dataframe through clean_products_data() first
        """
        def split_and_and_multiply(i :str) -> float:
            val_list = i.split(' x ')
            # print(val_list)
            return float(val_list[0]) * float(val_list[1])
    
        # remove spaces and stops from the end of the string
        products_data[weight_col] = products_data[weight_col].astype('str').str.replace('[ .]+$','',regex=True)

        ### find all things that end with kg
        kg_df = products_data[weight_col].astype('str').str.endswith('kg')
        # products_data.loc[kg_df,['weight_type']] = 'kg'
        products_data.loc[kg_df,[weight_col]] = products_data[kg_df][weight_col].astype('str').str.replace('kg','',regex=False) #.astype('float')

        ### find all things that end with g
        gram_df = products_data[weight_col].astype('str').str.endswith('g')
        # products_data.loc[gram_df,['weight_type']] = 'g'
        # 11 and 12 index - are 1.2 grams - that's clearly not right - fix?
        products_data.loc[gram_df,[weight_col]] = products_data[gram_df][weight_col].astype('str').str.replace('g','',regex=False) #.astype('float')

        # find all things that end ml
        ml_df = products_data[weight_col].astype('str').str.endswith('ml')
        # products_data.loc[ml_df,['weight_type']] = 'ml'
        products_data.loc[ml_df,[weight_col]] = products_data[ml_df][weight_col].astype('str').str.replace('ml','',regex=False) #astype('float')

        # find all things that end oz
        oz_df = products_data[weight_col].astype('str').str.endswith('oz')
        # products_data.loc[oz_df,['weight_type']] = 'oz'
        products_data.loc[oz_df,[weight_col]] = products_data[oz_df][weight_col].astype('str').str.replace('oz','',regex=False)

        # find all things that have a ' x ' in it, times it together
        times_df = products_data[weight_col].astype('str').str.contains(' x ')
        products_data.loc[times_df,[weight_col]] = products_data[times_df][weight_col].apply(split_and_and_multiply)

        ##conversions to kg
        # convert weight to float type
        products_data[weight_col] = products_data[weight_col].astype('float')
        # conversion on ml to kg
        products_data.loc[ml_df,[weight_col]] = products_data[ml_df][weight_col].apply(lambda x : x / 1000)
        # conversion on g to kg
        products_data.loc[gram_df,[weight_col]] = products_data[gram_df][weight_col].apply(lambda x : x / 1000)
        # conversion of oz to kg = 0.0283
        products_data.loc[oz_df,[weight_col]] = products_data[oz_df][weight_col].apply(lambda x: x * 0.0283)
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

        #remove bad data rows, remove nulls
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



