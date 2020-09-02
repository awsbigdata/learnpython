import boto3
import json

#use lambda role to create a sts
lclient = boto3.client('sts')
response = lclient.get_session_token()
print(json.dumps(response['Credentials']))