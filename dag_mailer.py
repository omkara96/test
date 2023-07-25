from datetime import datetime, timedelta
import os
import pandas as pd
import xlsxwriter

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

# Airflow DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def read_config_from_s3(bucket_name, s3_key, file_extension=".csv"):
    # Initialize S3Hook to read from S3 bucket
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Get the config file from S3
    file_content = s3_hook.read_key(s3_key=s3_key, bucket_name=bucket_name)
    
    # Convert the file content to a DataFrame
    df = pd.read_csv(pd.compat.StringIO(file_content))
    
    # Filter the DataFrame based on DAG_ID
    dag_id = df["DAG_ID"].iloc[0]
    dag_df = df[df["DAG_ID"] == dag_id]
    
    # Get the DAG last run details
    # You may need to adjust this part depending on how your DAG is named in Airflow
    from airflow.models import DagRun
    dag_runs = DagRun.find(dag_id=dag_id)
    last_dag_run = dag_runs[0] if dag_runs else None
    
    # Format the information into an Excel file
    output_file = f"/path/to/output/folder/{dag_id}_dag_run_details.xlsx"
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        dag_df.to_excel(writer, sheet_name="DAG_Run_Details", index=False)
        if last_dag_run:
            last_dag_run_df = pd.DataFrame({
                "DAG ID": [last_dag_run.dag_id],
                "Scheduled Time": [last_dag_run.execution_date],
                "Start Time": [last_dag_run.start_date],
                "End Time": [last_dag_run.end_date],
                "Total Time Required": [last_dag_run.duration],
                "DAG Status": [last_dag_run.state],
                "Error": [last_dag_run.get_error_message()],
            })
            last_dag_run_df.to_excel(writer, sheet_name="Last_DAG_Run_Details", index=False)

    return output_file

def send_email_with_attachment(**kwargs):
    # Extract the output file path from the previous task
    output_file = kwargs['task_instance'].xcom_pull(task_ids='read_config_from_s3_task')
    
    # Email settings
    subject = "Airflow DAG Run Details"
    to = ["recipient@example.com"]
    body = "Please find the attached Airflow DAG run details."

    # Send the email with the attachment
    send_email(to=to, subject=subject, html_content=body, files=[output_file])

# Define the DAG
with DAG(
    dag_id="dag_with_email",
    default_args=default_args,
    description="Airflow DAG to read config file from S3, fetch DAG run details, and send an email with the Excel attachment.",
    schedule_interval=None,  # Set your preferred schedule_interval
    catchup=False,  # Set to True if you want to catch up with missed DAG runs on backfill
) as dag:
    # Task to read config from S3 and fetch DAG run details
    read_config_task = PythonOperator(
        task_id="read_config_from_s3_task",
        python_callable=read_config_from_s3,
        op_args=["your_s3_bucket_name", "path/to/your/config_file.csv"],
        provide_context=True,
    )

    # Task to send an email with the Excel attachment
    send_email_task = PythonOperator(
        task_id="send_email_with_attachment_task",
        python_callable=send_email_with_attachment,
        provide_context=True,
    )

    # Set the task dependencies
    read_config_task >> send_email_task

# Note: Make sure to replace "your_s3_bucket_name" and "path/to/your/config_file.csv"
# with your actual S3 bucket name and config file path.
