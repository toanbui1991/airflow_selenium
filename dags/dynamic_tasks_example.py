from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator

from datetime import datetime

@task
def get_s3_files(current_prefix):
    s3_hook = S3Hook(aws_conn_id='s3')
    current_files = s3_hook.list_keys(bucket_name='my-bucket', prefix=current_prefix + "/", start_after_key=current_prefix + "/")
    return [[file] for file in current_files] #return a list of files


with DAG(dag_id='mapping_elt', 
        start_date=datetime(2022, 4, 2),
        catchup=False,
        template_searchpath='/usr/local/airflow/include',
        schedule_interval='@daily') as dag:
    #partial use for mapping
    copy_to_snowflake = S3ToSnowflakeOperator.partial(
        task_id='load_files_to_snowflake', 
        stage='MY_STAGE',
        table='COMBINED_HOMES',
        schema='MYSCHEMA',
        file_format="(type = 'CSV',field_delimiter = ',', skip_header=1)",
        snowflake_conn_id='snowflake').expand(s3_keys=get_s3_files(current_prefix="{{ ds_nodash }}"))

    move_s3 = S3CopyObjectOperator(
        task_id='move_files_to_processed',
        aws_conn_id='s3',
        source_bucket_name='my-bucket',
        source_bucket_key="{{ ds_nodash }}"+"/",
        dest_bucket_name='my-bucket',
        dest_bucket_key="processed/"+"{{ ds_nodash }}"+"/"
    )

    delete_landing_files = S3DeleteObjectsOperator(
        task_id='delete_landing_files',
        aws_conn_id='s3',
        bucket='my-bucket',
        prefix="{{ ds_nodash }}"+"/"
    )

    transform_in_snowflake = SnowflakeOperator(
        task_id='run_transformation_query',
        sql='/transformation_query.sql',
        snowflake_conn_id='snowflake'
    )

    copy_to_snowflake >> [move_s3, transform_in_snowflake]
    move_s3 >> delete_landing_files