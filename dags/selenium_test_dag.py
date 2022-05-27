import os
from airflow.models import DAG
from airflow.operators.selenium_plugin import SeleniumOperator

from selenium_scripts.scrape_youtube_channel import scrape_youtube_channel
from datetime import datetime, timedelta
import logging








date = '{{ ds_nodash }}'
file_name = 'episode_{}.mp3'.format(date)
bucket_name = 'wake_up_to_money'
key = os.path.join(bucket_name, file_name)
cwd = os.getcwd()
local_downloads = os.path.join(cwd, 'downloads')

default_args = {
    # 'wait_for_downstream': True,
    'schedule_interval': '0 7 * * *',
    'start_date': datetime(2022, 5, 26),
    'end_date': datetime(2022, 10, 27),
    'catchup': False
    }

with DAG('scrape_youtube', **default_args) as dag:

    channel_id = 'UCucot-Zp428OwkyRm2I7v2Q'
    scrape_youtube_channel = SeleniumOperator(
        script=scrape_youtube_channel,
        script_args=[channel_id],
        task_id='scrape_youtube_channel',
        dag=dag)

    scrape_youtube_channel



