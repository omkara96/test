######################################################################################################################################################################################################################
# Imports
######################################################################################################################################################################################################################
import io
import os
import re
import sys
import boto3
import json
import csv
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.db import create_session
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.utils.email import send_email
from datetime import datetime, timedelta, date
import time
from botocore.exceptions import ClientError

######################################################################################################################################################################################################################
# DAG Description
######################################################################################################################################################################################################################
# Created by: omkar
# Description: This DAG moves files from one S3 bucket to another S3 bucket based on the provided source and destination directories.
# All parameters are mandatory to run the DAG. Refer to the sample parameters below:
# Sample Parameter = {
#     "src_dir": "source_directory",
#     "destination_dir": "destination_directory",
#     "role_arn": "arn:aws:iam::1234567890:role/my-role",
#     "source_bucket_name": "source_bucket",
#     "destination_bucket_name": "destination_bucket",
#     "interface_desc": "Interface Description"
# }
######################################################################################################################################################################################################################
# Default workflow arguments
######################################################################################################################################################################################################################

input = {
    'dag_id': Variable.get("airflow_region_1") + "-" + Variable.get("aws_environment_code") + "-JP-DnA-FILE_MOVEMENT_S3",
    'aws_connection': Variable.get("aws_connection_1"),
    'aws_region_name': Variable.get("aws_region"),
    'email_notification': ['omkar.varma@merck.com', 'jp_hhie_dia_job@merck.com'],
    'start_date': None,
    'schedule_interval': None,
    'retry_count': -1,
    'sleep_time': 60
}
common_scripts_path = Variable.get("common_scripts_path")

today = date.today().strftime('%Y%m%d')

default_args = {
    'owner': 'airflow',
    'start_date': datetime.strptime(input.get('start_date'), '%Y-%m-%d %H:%M:%S') if input.get(
        'start_date') else days_ago(1),
    'email_on_failure': True,
    'email': input['email_notification'],
    'wait_for_downstream': True
}

def _get_s3_client(role_arn: str):
    """
    Returns boto3 s3 client
    """
    print("Role Arn Boto Function: " + role_arn) 
    connection = BaseHook.get_connection(input.get("aws_connection"))
    if role_arn is None:
        return boto3.client(
            "s3",
            aws_access_key_id=connection.login,
            aws_secret_access_key=connection.password
        )

    # Assume specified role
    stsresponse = boto3.client(
        'sts',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password
    ).assume_role(
        RoleArn=role_arn,
        RoleSessionName='newsession')

    return boto3.client(
        's3',
        aws_access_key_id=stsresponse["Credentials"]["AccessKeyId"],
        aws_secret_access_key=stsresponse["Credentials"]["SecretAccessKey"],
        aws_session_token=stsresponse["Credentials"]["SessionToken"]
    )

def move_files_to_s3(src_dir, destination_dir, role_arn: str, source_bucket_name, destination_bucket_name, interface_desc):
    # Connect to S3
    print("Source Directory [Files to be Archived from]: " + src_dir)
    print("Target Directory [Files will be Archived at]: " + destination_dir)
    print("ARN Role: " + role_arn)
    print("Source Bucket Name: " + source_bucket_name)
    print("Destination Bucket Name: " + destination_bucket_name)
    
    s3_client = _get_s3_client(role_arn)

    # List objects in the source directory
    response = s3_client.list_objects_v2(Bucket=source_bucket_name, Prefix=src_dir)

    # Get the list of files in the source directory
    file_list = [obj['Key'] for obj in response.get('Contents', [])]
    print("Files Found: ", file_list)

    # Move each file to the destination directory
    for file_key in file_list:
        source_key = f'{src_dir}/{file_key}'
        destination_key = f'{destination_dir}/{file_key}'
        s3_client.copy_object(Bucket=destination_bucket_name, CopySource={'Bucket': source_bucket_name, 'Key': source_key}, Key=destination_key)
        s3_client.delete_object(Bucket=source_bucket_name, Key=source_key)

    # Send an email with the list of moved files
    email_subject = f'Files Archived for {interface_desc}'
    email_body = f'The following files were moved from {src_dir} to {destination_dir}:\n\n'
    email_body += '\n'.join(file_list)
    print(email_body)

    send_email(email_subject, email_body)

def send_email(subject, body):
    email_operator = EmailOperator(
        task_id='send_email',
        to=input['email_notification'],
        subject=subject,
        html_content=f'<p>{body}</p>',
        dag=dag
    )
    email_operator.execute(context={})

# Define the DAG
dag = DAG(
        dag_id=input['dag_id'],
        default_args=default_args,
        description='Copy files from one S3 bucket to another based on the provided source and destination directories',
        schedule_interval=input['schedule_interval']
)

# Define the tasks
move_files_task = PythonOperator(
    task_id='move_files_to_s3',
    python_callable=move_files_to_s3,
    op_kwargs={'src_dir': '{{ dag_run.conf.src_dir }}', 'destination_dir': '{{ dag_run.conf.destination_dir }}', 'role_arn': '{{ dag_run.conf.role_arn }}', 'source_bucket_name': '{{ dag_run.conf.source_bucket_name }}', 'destination_bucket_name': '{{ dag_run.conf.destination_bucket_name }}', 'interface_desc': '{{ dag_run.conf.interface_desc }}'},
    dag=dag,
)

send_email_task = PythonOperator(
    task_id='send_email',
    python_callable=send_email,
    op_kwargs={'subject': 'Files Archived', 'body': 'Files have been archived.'},
    dag=dag,
)

# Set task dependencies
move_files_task >> send_email_task
