from datetime import date, datetime, timedelta
import pandas as pd
import requests
import json
import uuid
import os
from google.cloud import storage

def upload_data_to_gcs(bucket_name,destination_blob_name,source_file_name):
    storage_client=storage.Client()
    bucket=storage_client.bucket(bucket_name)
    blob=bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")



def fetch_news_data():
    today=date.today()
    api_key='3c97b0a4c9984e5abc5e97566ab386e1'

    base_url='https://newsapi.org/v2/everything?q={}&from={}&to={}&sortBy=popularity&apiKey={}&language=en'
    start_date_value=str(today - timedelta(days=1))
    to_date_value=str(today)

    df=pd.DataFrame(columns=['newsTitle','timestamp', 'author', 'url_to_source', 'source', 'content','urlToImage'])

    url_extractor=base_url.format('apple',start_date_value,to_date_value,api_key)
    response=requests.get(url_extractor)
    d=response.json()

    for i in d['articles']:
        newsTitle=i['title']
        timestamp=i['publishedAt']
        author=i['author']
        url_to_source=i['url']
        source=i['source']['name']
        urlToImage=i['urlToImage']
        partial_content=i['content'] if i['content'] is not None else ""

        if len(partial_content)>=200:
            trimmed_content=partial_content[:199]
        if '.' in partial_content:
            trimmed_content=partial_content[:partial_content.rindex('.')]
        else:
            trimmed_content=partial_content
    
        new_row= pd.DataFrame({
            'newsTitle':[newsTitle],
            'timestamp':[timestamp],
            'author':[author],
            'url_to_source':[url_to_source],
            'source':[source],
            'content':[trimmed_content],
            'urlToImage':[urlToImage]
        })

        df= pd.concat([df,new_row], ignore_index=False)
    
    current_time = datetime.now().strftime('%Y%m%d%H%M%S')
    file_name=f'run_{current_time}.parquet'
    print(df)

    print("Current working directory",os.getcwd())

    df.to_parquet(file_name)

    bucket_name='snowflake_project_de'
    destination_blob_name=f'news_api_project/parquet_files/{file_name}'
    upload_data_to_gcs(bucket_name,destination_blob_name,file_name)

    os.remove(file_name)
    