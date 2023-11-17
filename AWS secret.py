try:
    import os
    import json
    import boto3

    print("All Modules loaded ...", __name__)

except Exception as e:
    raise ("Some Modules are Missing :{} ".format(e))

print("Ok..... ")

global AWS_ACCESS_KEY
global AWS_SECRET_KEY
global AWS_REGION_NAME

AWS_ACCESS_KEY = "XXXX"
AWS_SECRET_KEY ="XXXX"
AWS_REGION_NAME = "us-east-1"


class AwsSecretManager(object):

    __slots__ = ["_session", "client"]

    def __init__(self):
        self._session = boto3.session.Session()
        self.client =  self. _session.client(
            service_name='secretsmanager',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION_NAME
        )

    def get_secrets(self, secret_name=''):
         try:
             if secret_name== '':raise Exception("Secret Name cannot be Null ")
             get_secret_value_response = self.client.get_secret_value(
                 SecretId=secret_name
             )
             if 'SecretString' in get_secret_value_response:
                 secret = get_secret_value_response['SecretString']
                 secret = json.loads(secret)
                 for key, value in secret.items():os.environ[key] = value

                 return {
                     "status":200,
                     "error":{},
                     "data":{
                         "message":True
                     }
                 }

         except Exception as e:

             return {
                 "status":-1,
                 "error":{
                     "message":str(e)
                 }
             }

print("Start ")
secrets_name = "XXXXX"
secret_manager = AwsSecretManager()
response_secrets = secret_manager.get_secrets(secret_name=secrets_name)
print(

    os.getenv("NAME")

)
