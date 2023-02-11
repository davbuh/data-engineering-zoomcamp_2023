--== Question 1 ===---
--What is the count for fhv vehicle records for year 2019?

select count(*)
from zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data
--43244696

--== Question 2 ===---
--Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
select count(distinct affiliated_base_number)
from zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data
-- 0 

select count(distinct affiliated_base_number)
from zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data_int
-- 317.94 MB

--== Question 3 ===---
--How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
select count(*)
from zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data_int
where PUlocationID is null and DOlocationID is null
--717748

--== Question 4 ===---
--What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
--partition by date(pickup_datetime) and clustered by affiliated_base_number
create or replace table zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data_par_cls 
partition by
(
  date(pickup_datetime)
)
cluster by affiliated_base_number
as
select *
from zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data_int

--== Question 5 ===---
--Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
--Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which --most closely matches.
select distinct affiliated_base_number
from zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data_int
where date(pickup_datetime) between '2019-03-01' and '2019-03-31'
--647.87

select distinct affiliated_base_number
from zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data_par_cls
where date(pickup_datetime) between '2019-03-01' and '2019-03-31'
--23.05

--== Question 6 ===---
--Where is the data stored in the External Table you created?
-- google cloud storage


--== Question 7 ===---
--It is best practice in Big Query to always cluster your data
--no



