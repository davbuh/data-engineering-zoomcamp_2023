--must create a dataset in same region the gcs is in (eur 6)
-- also query setting to execute in same region (not automatic - it will execute in us)

create or replace external table zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data
options
( 
  format = 'csv',
  uris = [
    'gs://dtc_data_lake_zoomcamp-de-202301/data/fhv/1.csv.gz'
    ,'gs://dtc_data_lake_zoomcamp-de-202301/data/fhv/2.csv.gz'
    ,'gs://dtc_data_lake_zoomcamp-de-202301/data/fhv/3.csv.gz'
    ,'gs://dtc_data_lake_zoomcamp-de-202301/data/fhv/4.csv.gz'
    ,'gs://dtc_data_lake_zoomcamp-de-202301/data/fhv/5.csv.gz'
    ,'gs://dtc_data_lake_zoomcamp-de-202301/data/fhv/6.csv.gz'
    ,'gs://dtc_data_lake_zoomcamp-de-202301/data/fhv/7.csv.gz'
    ,'gs://dtc_data_lake_zoomcamp-de-202301/data/fhv/8.csv.gz'
    ,'gs://dtc_data_lake_zoomcamp-de-202301/data/fhv/9.csv.gz'
    ,'gs://dtc_data_lake_zoomcamp-de-202301/data/fhv/10.csv.gz'
    ,'gs://dtc_data_lake_zoomcamp-de-202301/data/fhv/11.csv.gz'
    ,'gs://dtc_data_lake_zoomcamp-de-202301/data/fhv/12.csv.gz'
  ]
);

create or replace table zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data_int as
select *
from zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data

select *
from zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data limit 10

select *
from zoomcamp-de-202301.dezoomcamp_eur_6.fhv_taxi_data_int limit 10