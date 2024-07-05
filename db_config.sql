/***************************************************************
!! Task 1 Cast cols of the orders_table to the correct data types
****************************************************************/

ALTER TABLE public.orders_table ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
ALTER TABLE public.orders_table ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;
ALTER TABLE public.orders_table ALTER COLUMN card_number TYPE varchar(19) USING card_number::varchar;
ALTER TABLE public.orders_table ALTER COLUMN store_code TYPE varchar(12) USING store_code::varchar;
ALTER TABLE public.orders_table ALTER COLUMN product_code TYPE varchar(11) USING product_code::varchar;
ALTER TABLE public.orders_table ALTER COLUMN product_quantity TYPE smallint USING product_quantity::smallint;

/***************************************************************
!! Task 2 Cast the cols of dim_users to correct data types
****************************************************************/


ALTER TABLE public.dim_users ALTER COLUMN first_name TYPE varchar(255) USING first_name::varchar;
ALTER TABLE public.dim_users ALTER COLUMN last_name TYPE varchar(255) USING last_name::varchar;
ALTER TABLE public.dim_users ALTER COLUMN date_of_birth TYPE date USING date_of_birth::date;
ALTER TABLE public.dim_users ALTER COLUMN country_code TYPE varchar(2) USING country_code::varchar;
ALTER TABLE public.dim_users ALTER COLUMN join_date TYPE date USING join_date::date;
ALTER TABLE public.dim_users ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;


/***************************************************************
!! Task 3 - update dim_store_details
****************************************************************/


ALTER TABLE public.dim_store_details ALTER COLUMN locality TYPE varchar(255) USING locality::varchar;
ALTER TABLE public.dim_store_details ALTER COLUMN store_code TYPE varchar(12) USING store_code::varchar;
ALTER TABLE public.dim_store_details ALTER COLUMN staff_numbers TYPE smallint USING staff_numbers::smallint;
ALTER TABLE public.dim_store_details ALTER COLUMN opening_date TYPE date USING opening_date::date;
ALTER TABLE public.dim_store_details ALTER COLUMN store_type TYPE varchar(255) USING store_type::varchar;
ALTER TABLE public.dim_store_details ALTER COLUMN latitude TYPE float8  USING latitude::float8;
ALTER TABLE public.dim_store_details ALTER COLUMN country_code TYPE varchar(2) USING country_code::varchar;
ALTER TABLE public.dim_store_details ALTER COLUMN continent TYPE varchar(255) USING continent::varchar;

-- tidy up data 
UPDATE dim_store_details SET address = null WHERE address LIKE 'N/A';


/***************************************************************
!! Task 4 - make changes to dim_products table for delivery team 
****************************************************************/


UPDATE dim_products SET product_price = REPLACE(product_price,'£','') WHERE product_price LIKE '£%';

/* set up new col, spec as below:
+--------------------------+-------------------+
| weight_class VARCHAR  | weight range(kg)  |
+--------------------------+-------------------+
| Light                    | < 2               |
| Mid_Sized                | >= 2 - < 40       |
| Heavy                    | >= 40 - < 140     |
| Truck_Required           | => 140            |
+----------------------------+-----------------+
*/

ALTER TABLE public.dim_products ADD weight_class varchar(14) NULL;
UPDATE dim_products SET weight_class = case 
          when weight < 2 then 'Light'
          when weight >= 2 and weight < 40 then 'Mid_Sized'
          when weight >= 40 and weight < 140 then 'Heavy'
          when weight >= 140 then 'Truck_Required'
        end 
WHERE weight_class IS NULL ;


/***************************************************************
!! Task 5 update dim_products with the required data types
****************************************************************/


-- change col name
ALTER TABLE public.dim_products RENAME COLUMN removed TO still_available;

-- then alter to a bool type
ALTER TABLE public.dim_products ALTER COLUMN still_available TYPE bool USING CASE still_available 
  WHEN 'Still_avaliable' THEN True 
  WHEN 'Removed' THEN False END


ALTER TABLE public.dim_products ALTER COLUMN product_price TYPE float8 USING product_price::float8;
ALTER TABLE public.dim_products ALTER COLUMN weight TYPE float8 USING weight::float8;
ALTER TABLE public.dim_products ALTER COLUMN "EAN" TYPE varchar(17) USING "EAN"::varchar;
ALTER TABLE public.dim_products ALTER COLUMN product_code TYPE varchar(11) USING product_code::varchar;
ALTER TABLE public.dim_products ALTER COLUMN date_added TYPE date USING date_added::date;
ALTER TABLE public.dim_products ALTER COLUMN uuid TYPE uuid USING uuid::uuid;
ALTER TABLE public.dim_products ALTER COLUMN "weight_class" TYPE varchar(14) USING weight_class::varchar;

/***************************************************************
!! Task 6 - dim_date_times sort out table
****************************************************************/

ALTER TABLE public.dim_date_times ALTER COLUMN "month" TYPE varchar(2) USING "month"::varchar;
ALTER TABLE public.dim_date_times ALTER COLUMN "year" TYPE varchar(4) USING "year"::varchar;
ALTER TABLE public.dim_date_times ALTER COLUMN "day" TYPE varchar(2) USING "day"::varchar;
ALTER TABLE public.dim_date_times ALTER COLUMN time_period TYPE varchar(10) USING time_period::varchar;
ALTER TABLE public.dim_date_times ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;



/***************************************************************
!! Task 7 update dim_card_details
****************************************************************/

ALTER TABLE public.dim_card_details ALTER COLUMN card_number TYPE varchar(19) USING card_number::varchar;
ALTER TABLE public.dim_card_details ALTER COLUMN expiry_date TYPE varchar(5) USING expiry_date::varchar;
ALTER TABLE public.dim_card_details ALTER COLUMN date_payment_confirmed TYPE date USING date_payment_confirmed::date;


/***************************************************************
!! Task 8 Create Primary Keys in dimension tables
****************************************************************/

ALTER TABLE public.dim_date_times ADD CONSTRAINT dim_date_times_pk PRIMARY KEY (date_uuid);
ALTER TABLE public.dim_users ADD CONSTRAINT dim_users_pk PRIMARY KEY (user_uuid);
ALTER TABLE public.dim_store_details ADD CONSTRAINT dim_store_details_pk PRIMARY KEY (store_code);
ALTER TABLE public.dim_products ADD CONSTRAINT dim_products_pk PRIMARY KEY (product_code);
ALTER TABLE public.dim_card_details ADD CONSTRAINT dim_card_details_pk PRIMARY KEY (card_number);

/***************************************************************
!! Task 9 Finalise Star-based schema and adding the foreign keys to the order table
****************************************************************/

ALTER TABLE public.orders_table ADD CONSTRAINT orders_table_fk_dim_date_times FOREIGN KEY (date_uuid) REFERENCES public.dim_date_times(date_uuid);
ALTER TABLE public.orders_table ADD CONSTRAINT orders_table_fk_dim_users FOREIGN KEY (user_uuid) REFERENCES public.dim_users(user_uuid);
ALTER TABLE public.orders_table ADD CONSTRAINT orders_table_fk_store_details FOREIGN KEY (store_code) REFERENCES public.dim_store_details(store_code);
ALTER TABLE public.orders_table ADD CONSTRAINT orders_table_fk_dim_products FOREIGN KEY (product_code) REFERENCES public.dim_products(product_code);
ALTER TABLE public.orders_table ADD CONSTRAINT orders_table_fk_dim_card_details FOREIGN KEY (card_number) REFERENCES public.dim_card_details(card_number);
