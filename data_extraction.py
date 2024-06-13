from database_utils import DatabaseConnector
import pandas as pd



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


def test_databaseconnector():
    a = DataExtractor()
    a.read_rds_table('legacy_store_details')
    # print(a.databaseconnector.list_db_tables())


def test_bad_databaseconnector():
    a = DataExtractor()
    a.read_rds_table('thing')
    print(a.databaseconnector.list_db_tables())


if __name__ == '__main__':
    #test_db_connector()
    # test_init_db_engine()
    test_databaseconnector()