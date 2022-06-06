
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

def tweets_json(url):
    response = requests.request("GET", url, headers=headers)
    print(response.status_code)
    if response.status_code != 200:
        print(response.text)
    else:
        yield response.json()

def paginate_tweets(url):
    qurl = url
    while True:
        tweets_set = list(tweets_json(qurl))
        resp_dict = json.loads(json.dumps(tweets_set, indent=4))[0]
        yield from resp_dict['data']
        if "next_token" in resp_dict.get("meta", {}):
            next_token = resp_dict['meta']['next_token']
            qurl = f"{url}&next_token={next_token}"
        else:
            break

def _print_tweets(url,file_name):
    Path(Path(file_name).parent).mkdir(parents=True, exist_ok=True)
    with open(file_name, "w") as _file:
        all_tweets = list(paginate_tweets(url))
        #print(json.dumps(all_tweets, indent=4))
        json.dump(all_tweets, _file, ensure_ascii=True)
        _file.write("\n")
	
t1 = PythonOperator(
    task_id='get_tweets',
    python_callable=_print_tweets,
    op_args=[query_url,output_file],
    dag=dag)
