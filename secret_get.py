import boto3

# Replace these values with your own AWS access key ID and secret access key
access_key_id = 'YOUR_ACCESS_KEY_ID'
secret_access_key = 'YOUR_SECRET_ACCESS_KEY'

# The name of the secret in AWS Secrets Manager
secret_name = 'your/secret/name'

# Create a Secrets Manager client
session = boto3.session.Session()
secrets_client = session.client(
    service_name='secretsmanager',
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key,
    region_name='us-east-1'  # Replace with your desired region
)

try:
    # Retrieve the secret value
    response = secrets_client.get_secret_value(SecretId=secret_name)
    
    # If the secret has a string value, print it
    if 'SecretString' in response:
        secret_value = response['SecretString']
        print("Secret Value:", secret_value)
    else:
        print("Secret Value not found.")
    
except Exception as e:
    print("Error:", str(e))
