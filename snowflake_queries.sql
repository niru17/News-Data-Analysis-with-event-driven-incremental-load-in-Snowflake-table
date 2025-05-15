use role accountadmin;

create database news_api;

use news_api;

create file format parquet_format TYPE=parquet;

create or replace storage integration news_data_gcs_int
type=external_stage
storage_provider=gcs
enabled=true
storage_allowed_locations=('gcs://snowflake_project_de/news_api_project/parquet_files/')

desc integration news_data_gcs_int;

create or replace stage news_api_stage
url='gcs://snowflake_project_de/news_api_project/parquet_files/'
storage_integration=news_data_gcs_int
file_format=(type='parquet');

show stages;

select * from summary_news;