
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
##change the region as per yours

client = boto3.client('glue',region_name='us-east-1')

startTime = datetime.now()

try:
    client.delete_partition(DatabaseName='athenademo',TableName='drop_perform',PartitionValues=['69','1005004','1987-12-16'])
except ClientError as e:
    print('partition not exist',e.response['Error']['Code'])

#Python 2:
print(datetime.now() - startTime)