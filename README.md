﻿# multinational-retail-data-centralisation380

Scenario:

You work for a multinational company that sells various goods across the globe.
Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.
In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.....

This is an attempt to demonstrate how different datasets can be consolodated into a centralised location, and acts as a single source of truth for sales data.


## Installation

To grab the code:

```
git clone https://github.com/spence91-aicore/multinational-retail-data-centralisation380.git
```

### Dependancies

There are a lot of dependancies for this project because of the differnet data sources. Each source needs a different dependancy. This detail can be found in the `environment.yml` file.

```
conda env create -f .\environment.yml 
```

Please note - other package versions **may** work fine, but the versions outlined is what **has been tested**.

### Config

One of the routines in `data_extraction`, requires a configuration in order to connect. 
For ease-of-use, this can be entered in `db_creds.yaml`.

### Running

Routines to extract data from different sources, cleaned them, and then insert into a SQLite database are in `database_utils.py`. At the end of the file, uncomment the different data sources and to run them.

e.g 

```
if __name__ == '__main__':
    #test_db_connector()
    # test_init_db_engine()
    # test_upload_to_db()
    # test_upload_to_db_card_details() # UNCOMMENT ME OUT TO RUN
    # test_upload_to_db_store_data()
    # test_upload_todb_products_data() 
    # test_upload_todb_orders_data()   
    test_upload_todb_datetimes_data()
```

### File Structure

* `database_utils.py` has the `DatabaseConnector` class, with helpers to ease connections and functions to posgres and SQLite databases
* `data_extraction.py` has the `DataExtractor`, which functions to help with fetching data sources. All main functions return pandas `DataFrame` objects
* `data_cleaning.py` has the `DataCleaning`, and cleaning them. Please note that some of the data cleaning is specific to the data that has been enountered.

`multinational_imports.py`, has a range of example functions to demonstrate the various capabilities of the 3 files above.

SQL files:

* `db_config.sql` has a set of database configurations that have been used to configure the database used in `multinational_imports.py`.
* `db_query.sql` has a set of SQL queries that show how to get data back out from the database. Using a range of SQL functions.

## License

This code is **license free**, please do whatever you want with it.
