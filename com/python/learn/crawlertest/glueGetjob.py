
import boto3


client = boto3.client('glue',region_name='us-east-1')


response = client.get_connection(Name='mssqlec2')

print(response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL'])
print(response['Connection']['ConnectionProperties']['USERNAME'])
print(response['Connection']['ConnectionProperties']['PASSWORD'])