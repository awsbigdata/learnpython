#!/usr/bin/python

#
# This program used to list the partition from data catalog
#

import boto3
from sys import argv
from datetime import date, datetime

#dbname='da_athena'
#tablename='predicate_output'


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()


client = boto3.client('glue',region_name='us-east-1')

predicate="year='2010'"
response = client.get_partitions(
    DatabaseName='hive_glue',
    TableName='bm_cities',
    Expression=predicate
)

paths=[]

for partition in response['Partitions']:
    path="{}*".format(partition['StorageDescriptor']['Location'])
    paths.append(path)

