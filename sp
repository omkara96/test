import boto3
import os
import datetime

# Declaration session for variables
timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M')
pid = os.getpid()
current_date = datetime.datetime.now().strftime('%Y.%m.%d_%H.%M.%S')

working_folder = 'WorkFold'
data_file_dir = 'DataFiles'
conf_folder = 'File_split'
src_file_dir = 'File_split'

# Assign Parameters to variables
country = 'JP'

# Define your AWS access keys
access_key_id = 'your_access_key_id'
secret_access_key = 'your_secret_access_key'

# Define your S3 bucket name
bucket_name = 'your_bucket_name'

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
        src_file_contents = response['Body'].read().decode('utf-8').splitlines()

        # Splitting file process in a while loop
        start_row = 1
        end_row = sum(1 for _ in src_file_contents)  # source file split configure file
        start_column = 1
        end_column = len(src_file_contents[0].split(','))

        while start_row <= end_row:
            conf_file_key = os.path.join(conf_folder, 'CIM_ULTMARC_FILE.conf')
            response = s3.get_object(Bucket=bucket_name, Key=conf_file_key)
            conf_file_contents = response['Body'].read().decode('utf-8').splitlines()

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

            print(f"CLASSIFICATION_ID : {class_id} THIS FILE HAS GENERATED : {fname_time}")

            # Write the file count to S3
            cnt_value = sum(1 for line in split_file_contents)
            cnt_file_contents = f"{country},{fname_time},{cnt_value}\n"
            cnt_file_key = os.path.join(working_folder, 'ULTMARC_FILECOUNT_SPLITS.txt')
            s3.put_object(Body=cnt_file_contents, Bucket=bucket_name, Key=cnt_file_key, ContentType='text/plain')

            start_row += 1

if __name__ == "__main__":
    # Clear the existing files from temporary directories in S3
    print("Deleting previous splitted/Existing Temporary files at working directories")
    clear_existing_temp_files()

    # Start File Splitting
    print("Starting Ultmarc files splitting process...")
    split_file()
