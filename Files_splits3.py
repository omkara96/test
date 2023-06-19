import boto3
import csv
import configparser
import os
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
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def split_csv_from_s3(starting_word, s3_bucket, s3_key, config_file):
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read(config_file)

    # Connect to the S3 bucket
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    rows = response['Body'].read().decode('utf-8').split('\n')

    # Iterate through each row in the input CSV file
    for row in csv.reader(rows):
        # Determine the starting word of the row
        row_starting_word = row[0].split()[0]

        # Check if there is a configuration for the starting word
        if row_starting_word == starting_word:
            # Get the output filename from the configuration
            output_filename = config[starting_word]['filename']

            # Create the output file if it doesn't exist
            if not os.path.exists(output_filename):
                with open(output_filename, 'w', newline='') as outfile:
                    writer = csv.writer(outfile)
                    writer.writerow(row)

            # Append the row to the output file
            with open(output_filename, 'a', newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(row)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

with DAG(
    'split_csv_from_s3',
    default_args=default_args,
    schedule_interval=None,
) as dag:
    starting_words = ['StartingWord1', 'StartingWord2']
    s3_bucket = 'my-s3-bucket'
    s3_key = 'path/to/my/csv/file.csv'
    config_file = 'path/to/my/config.ini'

    for starting_word in starting_words:
        task_id = f'split_csv_{starting_word}'
        op = PythonOperator(
            task_id=task_id,
            python_callable=split_csv_from_s3,
            op_kwargs={
                'starting_word': starting_word,
                's3_bucket': s3_bucket,
                's3_key': s3_key,
                'config_file': config_file,
            },
        )
