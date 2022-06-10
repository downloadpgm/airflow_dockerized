
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import os
import requests
import json
from pathlib import Path

# get this bearer token in Twitter developer portal https://developer.twitter.com/en/portal/dashboard
bearer_token = "xxxx"
headers = {"Authorization": "Bearer {}".format(bearer_token)}

query = "AluraOnline"
tweet_fields = "tweet.fields=author_id,created_at,text"
query_url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}".format(query,tweet_fields)
output_file = "/tmp/alura/extract_date={{ ds }}/twitter_{{ ds_nodash }}.json"

dag = DAG('twitter_retriever',
    start_date=datetime.now(),
    schedule_interval=None)

def paginate_tweets(url):
    qurl = url
    while True:
        response = requests.request("GET", qurl, headers=headers)
        #print(response.status_code)
        if response.status_code != 200:
		        break
        else:
            tweets_set = response.json()
            yield tweets_set
            if "next_token" in tweets_set['meta']:
                next_token = tweets_set['meta']['next_token']
                qurl = f"{url}&next_token={next_token}"
            else:
                break

def print_tweets(url,file_name):
    Path(Path(file_name).parent).mkdir(parents=True, exist_ok=True)
    all_tweets = paginate_tweets(url)
    with open(file_name, "w") as _file:
        for tweet_set in all_tweets:
            tweet_arr = tweet_set['data']
            for tweet in tweet_arr:
                json.dump(tweet, _file, ensure_ascii=False)
                _file.write("\n")
	
t1 = PythonOperator(
    task_id='get_tweets',
    python_callable=print_tweets,
    op_args=[query_url,output_file],
    dag=dag)
