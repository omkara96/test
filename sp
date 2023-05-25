import os
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
import boto3

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['your_email@example.com'],  # Update with recipient email address(es)
    'email_on_failure': True,
    'email_on_success': True,
}

# Define your AWS access keys
access_key_id = 'YOUR_ACCESS_KEY_ID'
secret_access_key = 'YOUR_SECRET_ACCESS_KEY'

# Define your S3 bucket name
bucket_name = 'YOUR_BUCKET_NAME'

# Initialize the S3 client with your access keys
s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

def clear_existing_temp_files():
    # Clear existing source files in the working folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=working_folder)
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.txt'):
                s3.delete_object(Bucket=bucket_name, Key=key)

    # Clear existing data files in the data file directory
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=data_file_dir)
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.txt'):
                s3.delete_object(Bucket=bucket_name, Key=key)

def split_file():
    src_file_nm = ''
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=src_file_dir)
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('_ZXW_ULT_VAN.dat'):
                src_file_nm = key
                break

    if src_file_nm:
        # Read the contents of the source file from S3
        response = s3.get_object(Bucket=bucket_name, Key=src_file_nm)
        src_file_contents = response['Body'].read().decode('cp932').splitlines()

        conf_file_key = 'Inbound/Ultmarc_cust_3000/JP/config/HHIE_ULTMARC_FILE.conf'
        response = s3.get_object(Bucket=bucket_name, Key=conf_file_key)
        conf_file_contents = response['Body'].read().decode('cp932').splitlines()

        # Splitting file process in a while loop
        start_row = 1
        end_row = sum(1 for _ in conf_file_contents)  # source file split configure file
        start_column = 1
        end_column = len(conf_file_contents[0].split(','))

        while start_row <= end_row:
            row = conf_file_contents[start_row-1]  # get the row values from configure file
            class_id = row.split(',')[0]  # based on the first field in the file will split
            fname = row.split(',')[1].strip()  # filenames from the .conf file
            fname_time = f"{fname}_{datetime.datetime.now().strftime('%Y%m%d%H%M')}.txt"

            split_file_contents = []
            for line in src_file_contents:
                if line.split(',')[0] == class_id:
                    split_file_contents.append(line + '\n')

            # Write the split file to S3
            split_file_key = os.path.join(working_folder, fname_time)
            s3.put_object(Body=''.join(split_file_contents), Bucket=bucket_name, Key=split_file_key, ContentType='text/plain')

            print(f"CLASSIFICATION_ID: {class_id}, THIS FILE HAS GENERATED: {fname_time}")

            # Write the file count to S3
            cnt_value = sum(1 for line in split_file_contents)
            cnt_file_contents = f"{country},{fname_time},{cnt_value}\n"
            cnt_info.append(cnt_file_contents)

            start_row += 1
        cnt_file_key = os.path.join(working_folder, 'ULTMARC_FILECOUNT_SPLITS.txt')
        s3.put_object(Body=''.join(cnt_info), Bucket=bucket_name, Key=cnt_file_key, ContentType='text/plain')

def send_success_email():
    return f"Splitting process completed successfully.\n\nFile Count Information:\n{''.join(cnt_info)}"

def send_failure_email(context):
    return f"Splitting process failed. Error: {context.get('exception')}"

# Declaration session for variables
timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M')
pid = os.getpid()
current_date = datetime.datetime.now().strftime('%Y.%m.%d_%H.%M.%S')

working_folder =  'Inbound/Ultmarc_cust_3000/JP/Datafiles/'
data_file_dir =  'Inbound/Ultmarc_cust_3000/JP/Datafiles/DT'
conf_folder = 'Inbound/Ultmarc_cust_3000/JP/config/'
src_file_dir = 'Inbound/Ultmarc_cust_3000/JP/Datafiles/'

# Assign Parameters to variables
country = 'JP'
cnt_info = []

# DAG definition
with DAG(
    'ultmarc_file_splitting',
    default_args=default_args,
    description='DAG for Ultmarc file splitting',
    schedule_interval=None,
    catchup=False,
) as dag:

    clear_temp_files_task = PythonOperator(
        task_id='clear_temp_files',
        python_callable=clear_existing_temp_files
    )

    split_file_task = PythonOperator(
        task_id='split_file',
        python_callable=split_file
    )

    success_email_task = EmailOperator(
        task_id='send_success_email',
        subject='Ultmarc File Splitting - Success',
        to='your_email@example.com',  # Update with recipient email address(es)
        html_content=send_success_email
    )

    failure_email_task = EmailOperator(
        task_id='send_failure_email',
        subject='Ultmarc File Splitting - Failure',
        to='your_email@example.com',  # Update with recipient email address(es)
        html_content=send_failure_email
    )

    clear_temp_files_task >> split_file_task >> success_email_task
    split_file_task >> failure_email_task
