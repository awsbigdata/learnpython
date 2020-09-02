#!/usr/bin/python

#
# This program used to sync the glue table schema and partition upto 1000
#
import boto3
from sys import argv
import json
from datetime import date, datetime

dbname='hive_glue'
tablename='optimizely_events_data'


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
#dbname=argv[1]
#tablename=argv[2]

client = boto3.client('glue',region_name='us-east-1')

response = client.get_table(
    DatabaseName=dbname,
    Name=tablename
)
print(response)
storagedes=response['Table']['StorageDescriptor']

partitionInput={}
partitionInput['Values']=['8317223810','events','2020-05-25','Main_Nav_javascript%3void(0);']
partitionInput['StorageDescriptor']=storagedes
partitionInput['StorageDescriptor']['Location']="s3://athenaiad/optimizely-events-data/v1/account_id=8317223810/type=events/date=2020-05-23/event=Main_Nav_javascript%3Avoid(0);/"

response = client.create_partition(DatabaseName=dbname, TableName=tablename,
                                     PartitionInput=partitionInput)

print(response)