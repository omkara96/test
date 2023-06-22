from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.email_operator import EmailOperator

def move_files_to_s3(src_dir, destination_dir):
    # Connect to S3
    s3_hook = S3Hook()

    # Get the list of files in the source directory
    file_list = s3_hook.list_keys(bucket_name='your_bucket_name', prefix=src_dir)

    # Move each file to the destination directory
    for file_key in file_list:
        source_key = f'{src_dir}/{file_key}'
        destination_key = f'{destination_dir}/{file_key}'
        s3_hook.copy_object(source_bucket_name='your_bucket_name', source_bucket_key=source_key,
                            dest_bucket_name='your_bucket_name', dest_bucket_key=destination_key)
        s3_hook.delete_objects(bucket='your_bucket_name', keys=[source_key])

    # Send an email with the list of moved files
    email_subject = 'Files Archived'
    email_body = f'The following files were moved from {src_dir} to {destination_dir}:\n\n'
    email_body += '\n'.join(file_list)

    send_email(email_subject, email_body)

def send_email(subject, body):
    # Code to send an email using your preferred email provider or SMTP server
    # Replace this with your own email sending implementation
    pass

# Define the DAG
dag = DAG(
    dag_id='s3_file_archival',
    start_date=datetime(2023, 6, 22),
    schedule_interval=None,
)

# Define the tasks
move_files_task = PythonOperator(
    task_id='move_files_to_s3',
    python_callable=move_files_to_s3,
    op_kwargs={'src_dir': '{{ dag_run.conf.src_dir }}', 'destination_dir': '{{ dag_run.conf.destination_dir }}'},
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
