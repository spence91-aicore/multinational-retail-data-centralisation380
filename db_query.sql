
/*
!! Task 1 - how many stores does the business have

The Operations team would like to know which countries we currently operate in and which country now has the most stores. Perform a query on the database to get the information, it should return the following information:

+----------+-----------------+
| country  | total_no_stores |
+----------+-----------------+
| GB       |             265 |
| DE       |             141 |
| US       |              34 |
+----------+-----------------+
Note: DE is short for Deutschland(Germany)
*/

SELECT 
country_code
, count(country_code) as total_no_stores 
fROM dim_store_details dsd 
GROUP bY country_code 
ORDER BY count(country_code) DESC 




------------------------------------------------------

/*
!! Task 2 - How mnay stores does the buiness have and in which countries

The business stakeholders would like to know which locations currently have the most stores.

They would like to close some stores before opening more in other locations.

Find out which locations have the most stores currently. The query should return the following:

+-------------------+-----------------+
|     locality      | total_no_stores |
+-------------------+-----------------+
| Chapletown        |              14 |
| Belper            |              13 |
| Bushley           |              12 |
| Exeter            |              11 |
| High Wycombe      |              10 |
| Arbroath          |              10 |
| Rutherglen        |              10 |
+-------------------+-----------------+
*/


SELECT 
locality 
,count(locality) as total_no_stores
FROM dim_store_details dsd 
GROUP BY locality 
ORDER BY count(locality) DESC



------------------------------------------------------

/*
!! Task 3 - Which months produced the largest amount of sales

Query the database to find out which months have produced the most sales. The query should return the following information:

+-------------+-------+
| total_sales | month |
+-------------+-------+
|   673295.68 |     8 |
|   668041.45 |     1 |
|   657335.84 |    10 |
|   650321.43 |     5 |
|   645741.70 |     7 |
|   645463.00 |     3 |
+-------------+-------+
*/

SELECT 
round(cast(sum(dp.product_price * ot.product_quantity) AS numeric), 2 ) AS total_sales
,ddt."month" 
FROM orders_table ot 
LEFT JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid 
LEFT JOIN dim_products dp ON ot.product_code = dp.product_code 
GROUP BY ddt."month"
ORDER BY round(cast(sum(dp.product_price * ot.product_quantity)as numeric), 2 ) DESC 

------------------------------------------------------

/*
!! Task 4 - How many sales are coming from online?

The company is looking to increase its online sales.

They want to know how many sales are happening online vs offline.

Calculate how many products were sold and the amount of sales made for online and offline purchases.

You should get the following information:

+------------------+-------------------------+----------+
| numbers_of_sales | product_quantity_count  | location |
+------------------+-------------------------+----------+
|            26957 |                  107739 | Web      |
|            93166 |                  374047 | Offline  |
+------------------+-------------------------+----------+
*/

SELECT 
count(1) AS numbers_of_sales
,sum(ot.product_quantity) AS product_quantity_count
, CASE WHEN dsd.store_type = 'Web Portal' THEN 'Web' ELSE 'Offline' END AS location
FROM orders_table ot 
LEFT JOIN dim_products dp ON ot.product_code = dp.product_code 
LEFT JOIN dim_store_details dsd ON ot.store_code = dsd.store_code 
GROUP BY CASE WHEN dsd.store_type = 'Web Portal' THEN 'Web' ELSE 'Offline' END 
ORDER BY count(1)


------------------------------------------------------

/*
!! Task 5 - What percentage of sales come threough each type of store

The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.

Find out the total and percentage of sales coming from each of the different store types.

The query should return:

+-------------+-------------+---------------------+
| store_type  | total_sales | percentage_total(%) |
+-------------+-------------+---------------------+
| Local       |  3440896.52 |               44.87 |
| Web portal  |  1726547.05 |               22.44 |
| Super Store |  1224293.65 |               15.63 |
| Mall Kiosk  |   698791.61 |                8.96 |
| Outlet      |   631804.81 |                8.10 |
+-------------+-------------+---------------------+
*/


SELECT 
sbs.store_type
,round(cast(sum(sbs.total_sales) OVER (partition BY store_type) AS numeric),2) AS total_sales 
,round(cast((sbs.total_sales / sum(sbs.total_sales) OVER ())*100 AS numeric),2) AS "percentage_total(%)"
FROM 
(
  SELECT 
  dsd.store_type 
  ,sum(dp.product_price * ot.product_quantity) AS total_sales
  FROM orders_table ot 
  LEFT JOIN dim_products dp ON ot.product_code = dp.product_code 
  LEFT JOIN dim_store_details dsd ON ot.store_code = dsd.store_code 
  GROUP BY dsd.store_type 
) as sbs -- sbs = sales_by_store 
ORDER BY sbs.total_sales / sum(sbs.total_sales) over () DESC 


------------------------------------------------------
/*
!! Task 6 - Which month in each year produced the highest cost of sales

The company stakeholders want assurances that the company has been doing well recently.

Find which months in which years have had the most sales historically.

The query should return the following information:

+-------------+------+-------+
| total_sales | year | month |
+-------------+------+-------+
|    27936.77 | 1994 |     3 |
|    27356.14 | 2019 |     1 |
|    27091.67 | 2009 |     8 |
|    26679.98 | 1997 |    11 |
|    26310.97 | 2018 |    12 |
|    26277.72 | 2019 |     8 |
|    26236.67 | 2017 |     9 |
|    25798.12 | 2010 |     5 |
|    25648.29 | 1996 |     8 |
|    25614.54 | 2000 |     1 |
+-------------+------+-------+
*/

SELECT 
sum(dp.product_price * ot.product_quantity) AS total_sales
,ddt."year"
,ddt."month" 
FROM orders_table ot 
LEFT JOIN dim_products dp ON ot.product_code = dp.product_code 
LEFT JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid 
GROUP BY ddt."month", ddt."year" 
ORDER BY sum(dp.product_price * ot.product_quantity) DESC 

------------------------------------------------------
/*
!! Task 7 - What is our staff headcount?

The operations team would like to know the overall staff numbers in each location around the world. Perform a query to determine the staff numbers in each of the countries the company sells in.

The query should return the values:

+---------------------+--------------+
| total_staff_numbers | country_code |
+---------------------+--------------+
|               13307 | GB           |
|                6123 | DE           |
|                1384 | US           |
+---------------------+--------------+
*/

SELECT 
sum(dsd.staff_numbers)
,dsd.country_code 
FROM dim_store_details dsd  
GROUP BY country_code 
ORDER BY sum(dsd.staff_numbers) DESC 



------------------------------------------------------
/*
!! Task 8 - Which German store type is selling the most?

The sales team is looking to expand their territory in Germany. Determine which type of store is generating the most sales in Germany.

The query will return:

+--------------+-------------+--------------+
| total_sales  | store_type  | country_code |
+--------------+-------------+--------------+
|   198373.57  | Outlet      | DE           |
|   247634.20  | Mall Kiosk  | DE           |
|   384625.03  | Super Store | DE           |
|  1109909.59  | Local       | DE           |
+--------------+-------------+--------------+
*/

SELECT 
round(cast(SUM(dp.product_price * ot.product_quantity) as numeric),2) AS total_sales
,dsd.store_type 
,dsd.country_code 
FROM orders_table ot 
LEFT JOIN dim_products dp ON ot.product_code = dp.product_code 
LEFT JOIN dim_store_details dsd ON ot.store_code = dsd.store_code 
WHERE dsd.country_code = 'DE'
GROUP BY dsd.country_code, dsd.store_type 
ORDER BY SUM(dp.product_price * ot.product_quantity) ASC


------------------------------------------------------
/*
!! Task 9 - How quickly is the company making sales?

Sales would like the get an accurate metric for how quickly the company is making sales.

Determine the average time taken between each sale grouped by year, the query should return the following information:

 +------+-------------------------------------------------------+
 | year |                           actual_time_taken           |
 +------+-------------------------------------------------------+
 | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "millise... |
 | 1993 | "hours": 2, "minutes": 15, "seconds": 35, "millise... |
 | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "millise... | 
 | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "millise... |
 | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "millise... |
 +------+-------------------------------------------------------+

Hint: You will need the SQL command LEAD.

*/

SELECT 
year,
AVG(diff) AS actual_time_taken
FROM 
(
  SELECT 
  ddt."year" 
  ,ddt."month" 
  ,(lead(ddt.datetime) OVER (partition BY ddt."year"  ORDER BY ddt.datetime) - ddt.datetime ) as diff 
  FROM orders_table ot 
  LEFT JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid 
) as ytd -- (y)ear (t)ime (d)elta
GROUP by year
ORDER BY AVG(diff) DESC 

