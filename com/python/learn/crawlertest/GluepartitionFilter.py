#!/usr/bin/python

#
# This program used to sync the glue table schema and partition upto 1000
#
import boto3
from sys import argv
import json
from datetime import date, datetime

dbname='default'
tablename='test1'


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
#dbname=argv[1]
#tablename=argv[2]

client = boto3.client('glue',region_name='us-east-1')

response = client.delete_partition(
    DatabaseName=dbname,
    TableName=tablename,PartitionValues=['__HIVE_DEFAULT_PARTITION__']
)

print(response)