import os
from airflow.models import DAG
from airflow.operators.selenium_plugin import SeleniumOperator
from selenium_scripts.scrape_youtube_channel import scrape_youtube_channel
from selenium_scripts.config import config
from datetime import datetime, timedelta
import logging
import pymysql










default_args = {
    # 'wait_for_downstream': True,
    'schedule_interval': '0 7 * * *',
    'start_date': datetime(2022, 5, 26),
    'end_date': datetime(2022, 10, 27),
    'catchup': False
    }



connection = pymysql.connect(**config.get("mysql_dev_admin"))
with connection:
    with connection.cursor as cursor:
        try:
            sql = """SELECT channel_id FROM tb_youtube_chn_info"""
            cursor.execute(sql)
            channel_ids = cursor.fetchall() #return tuple of tuple
            connection.commit() #default is not autocommit
        except Exception as e:
            connection.rollback()
            raise e


with DAG('scrape_youtube', **default_args) as dag:

    

    #create tasks 
    channel_ids = channel_ids[:20]
    tasks = []
    for item in channel_ids:
         
        channel_id = item[0]
        task = SeleniumOperator(
            script=scrape_youtube_channel,
            script_args=[channel_id],
            task_id='scrape_youtube_channel',
            dag=dag)
        tasks.append(task)
    
    #send task to workers
    for task in tasks:
        task


