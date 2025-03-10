import json

import boto3
from google.cloud import bigquery
import pandas as pd
import pendulum

from config import (
    BUCKET_NAME,
    PROJECT_ID,
    DATASET_ID,
    LOCATION
)


def upload_s3(raw_data, file_name):
    client = boto3.client('s3')
    date = pendulum.now('Asia/Seoul').format('YYYYMMDD')
    json_data = json.dumps(raw_data)
    client.put_object(
        Bucket=f"{BUCKET_NAME}",
        Key=f"{date}/{file_name}",
        Body=json_data,
        ContentType="application/json"
    )


def upload_bq(stats, table_id):
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    schema = client.get_table(table_ref).schema
    target_date = stats[0]['date'].replace('-', '')
    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        schema=schema,
        write_disposition="WRITE_TRUNCATE"
    )
    job = client.load_table_from_json(stats, f"{table_ref}${target_date}", job_config=job_config)
    job.result()


def upload(data):
    upload_s3(data['members'], "members.json")
    upload_s3(data['newsletters'], "newsletters.json")
    upload_bq(data['member_stats'], "blog_member_statistics")
    upload_bq(data['subscriber_stats'], "blog_subscriber_statistics")
    upload_bq(data['newsletter_stats'], "blog_newsletter_statistics")