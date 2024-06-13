import pandas as pd
from dateutil.parser import parse
from dateutil.parser import ParserError
import datetime

class DataCleaning:
    """methods to clean data from each of the data sources"""

    @staticmethod
    def _tidy_dates(date_col:pd.DataFrame) -> pd.DataFrame:
        @staticmethod
        def process_dates(input:str) -> datetime.datetime:
            try:
                return parse(input)
            except ParserError as e:
                # print(e)
                if 'Unknown string format' in e.__str__():
                    return None
                raise e
        
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
        """removes rows where there appears to be a 10char string in each col"""
        regex_for_bad_rows = '^[0-9A-Z]{10}$'
        match_mask : pd.Series = (user_data['first_name'].str.match(regex_for_bad_rows) & \
                user_data['last_name'].str.match(regex_for_bad_rows) & \
                user_data['company'].str.match(regex_for_bad_rows) & \
                user_data['email_address'].str.match(regex_for_bad_rows))
        return user_data.drop(user_data[match_mask].index)

    @staticmethod
    def _remove_rows_of_null_data(user_data:pd.DataFrame) -> pd.DataFrame:
        "removes rows where the first and last name are 'NULL'"
        match_mask : pd.Series = ( user_data['first_name'] == 'NULL' ) & ( user_data['last_name'] == 'NULL')
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
            if number.startswith('(') and ')' in number: # get rid of parentheses if 
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
        user_data = self._remove_rows_of_bad_data(user_data)
        user_data = self._remove_rows_of_null_data(user_data)
        user_data['date_of_birth'] = self._tidy_dates(user_data['date_of_birth'])
        user_data['join_date'] = self._tidy_dates(user_data['join_date'])
        user_data['country_code'] = user_data.apply(self._country_code_convert_ggb,axis=1)    
        user_data['phone_number'] = user_data.apply(self._cleanse_phone_data,axis=1) # must be run after country_code cleanse!
        return user_data



def test_clean_user_data():
    df : pd.DataFrame = pd.read_pickle('legacy_user.pkl')
    a = DataCleaning()
    df = a.clean_user_data(df)
    df.info()
    df.to_clipboard()


if __name__ == '__main__':
    test_clean_user_data()
# df = a.read_rds_table('legacy_users')
# df.to_pickle('legacy_user.pkl')