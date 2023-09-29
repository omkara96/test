import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def get_secret_value(secret_name, region_name):
    try:
        # Create a Secrets Manager client
        client = boto3.client('secretsmanager', region_name=region_name)

        # Retrieve the secret value
        response = client.get_secret_value(SecretId=secret_name)

        # If the secret has a 'SecretString' key, it's a string-based secret
        if 'SecretString' in response:
            secret_value = response['SecretString']
            return secret_value
        else:
            raise Exception("Secret value is not a string.")

    except NoCredentialsError:
        print("AWS credentials not found. Make sure you've configured your IAM credentials.")
    except PartialCredentialsError:
        print("Incomplete AWS credentials found. Make sure you've configured your IAM credentials correctly.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    # Replace these with your AWS IAM credentials and the name of the secret you want to fetch.
    access_key_id = 'YOUR_ACCESS_KEY_ID'
    secret_access_key = 'YOUR_SECRET_ACCESS_KEY'
    secret_name = 'YOUR_SECRET_NAME'
    aws_region = 'YOUR_AWS_REGION'

    # Set the AWS IAM credentials
    boto3.setup_default_session(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

    # Fetch the secret value
    secret_value = get_secret_value(secret_name, aws_region)

    if secret_value:
        print(f"Secret Value: {secret_value}")
