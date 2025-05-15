from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from fetch_news import fetch_news_data
from datetime import datetime, timedelta,date

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'retries':0,
    'email_on_failure':False,
    'email_on_retry':False,
    'retry_delay':timedelta(minutes=5),
}

dag = DAG(
    'news_api_to_gcs',
    default_args=default_args,
    description='Fetch news article and push to GCS bucket as parquet files',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025,5,15),
    catchup=False,
    tags=['dev'],
)

fetch_news_task = PythonOperator(
    task_id='Run_Python_FetchNewsData',
    python_callable=fetch_news_data,
    dag=dag,
)

snowflake_create_table =  SnowflakeOperator(
    task_id="Create_Table_in_Snowflake",
    sql="""CREATE TABLE IF NOT EXISTS news_api.PUBLIC.news_api_data USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(INFER_SCHEMA (
                    LOCATION => '@news_api.PUBLIC.news_api_stage',
                    FILE_FORMAT => 'parquet_format'
                ))
            )""",
    snowflake_conn_id="snowflake_conn",
    dag=dag,
)

snowflake_copy = SnowflakeOperator(
    task_id="Copy_Data_into_Table",
    sql=""" COPY INTO news_api.PUBLIC.news_api_data 
            FROM @news_api.PUBLIC.news_api_stage
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
            FILE_FORMAT = (FORMAT_NAME='parquet_format')
            """,
    snowflake_conn_id="snowflake_conn",
    dag=dag,
)

news_summary_task = SnowflakeOperator(
    task_id="create_or_replace_newssummary_tb",
    sql="""
        CREATE OR REPLACE TABLE news_api.PUBLIC.summary_news AS
        SELECT
            "source" as source,
            count(*) as article_count,
            MAX("timestamp") as latest_article_date,
            MIN("timestamp") as earliest_article_date
        FROM news_api.PUBLIC.news_api_data as tb
        GROUP BY source
        ORDER BY article_count DESC;
    """,
    snowflake_conn_id="snowflake_conn",
    dag=dag,
)

author_activity_task = SnowflakeOperator(
    task_id="create_author_activity_tb",
    sql="""
        CREATE OR REPLACE TABLE news_api.PUBLIC.author_activity AS
        SELECT
            "author",
            count(*) as article_count,
            MAX("timestamp") as latest_article_date,
            COUNT(DISTINCT "source") as distinct_sources
        FROM news_api.PUBLIC.news_api_data as tb
        GROUP BY "author"
        ORDER BY article_count DESC;
    """,
    snowflake_conn_id="snowflake_conn",
    dag=dag,
)

fetch_news_task>>snowflake_create_table>>snowflake_copy
snowflake_copy>>[news_summary_task,author_activity_task]





