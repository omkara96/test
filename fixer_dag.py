from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.amazon.aws.operators.s3_file_transform import S3FileTransformOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import re


def fix_delimited_file(bucket_name, input_file, output_file, delimiter):
    encodings = ['utf-8', 'shift_jis', 'cp932']  # Add more encodings if needed

    for encoding in encodings:
        try:
            s3_hook = S3Hook(aws_conn_id='your_aws_connection_id')
            lines = s3_hook.read_key(f'{bucket_name}/{input_file}', encoding=encoding).splitlines()

            fixed_lines = []
            prev_line = lines[0].replace('\n', '').replace('\r', '')  # Initialize prev_line with the first line

            for line in lines[1:]:
                line = line.replace('\n', '').replace('\r', '')
                words = line.split(delimiter)

                if len(words) < 5:
                    prev_line += line
                else:
                    fixed_lines.append(prev_line)
                    prev_line = line

            fixed_lines.append(prev_line)

            s3_hook.load_string('\n'.join(fixed_lines), f'{bucket_name}/{output_file}', replace=True)

            # Delete the input file
            s3_hook.delete_objects(bucket_name=bucket_name, keys=[input_file])

            # Rename the output file to the input file name
            s3_hook.copy_object(bucket_name=bucket_name, source_key=output_file, dest_key=input_file)

            print(f'Successfully processed the file with encoding: {encoding}')
            return

        except UnicodeDecodeError:
            pass

    print('Unable to process the file with any of the specified encodings.')


def process_file(bucket_name, regex_pattern):
    s3_hook = S3Hook(aws_conn_id='your_aws_connection_id')
    files = s3_hook.list_keys(bucket_name=bucket_name, prefix='your_prefix', delimiter='/')
    matching_files = [file for file in files if re.match(regex_pattern, file)]
    most_recent_file = max(matching_files, key=lambda x: s3_hook.get_key(x).last_modified)

    input_file = most_recent_file
    output_file = 'fixed_' + input_file

    delimiter = '\t'  # Replace with the delimiter used in your file

    fix_delimited_file(bucket_name, input_file, output_file, delimiter)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 3),
}

with DAG('process_delimited_file', default_args=default_args, schedule_interval='@daily') as dag:
    bucket_name = 'your_bucket_name'  # Replace with your bucket name

    regex_pattern = Variable.get('regex_pattern')

    process_file_task = PythonOperator(
        task_id='process_file',
        python_callable=process_file,
        op_kwargs={'bucket_name': bucket_name, 'regex_pattern': regex_pattern},
    )

process_file_task
