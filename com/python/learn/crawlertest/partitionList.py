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
# dbname=argv[1]
# tablename=argv[2]
# region=argv[3]

dbname="hive_glue"
tablename="bm_cities"
region="us-east-1"

client = boto3.client('glue',region_name=region)

response = client.get_table(
    DatabaseName=dbname,
    Name=tablename
)

tabledes=response['Table']


def partitionConstract(partitions,partitionValue):
    index=0
    part=""
    for partition in partitions:
        part+="{0}={1}/".format(partition['Name'],partitionValue[index])
        index=index+1
    return part

print('Partition list started')

def printPartition(nextToken=""):
    res = client.get_partitions(DatabaseName=dbname, TableName=tablename, MaxResults=1000,NextToken=nextToken)
    for epart in res['Partitions']:
        print(epart)
    return res


res=printPartition()

while 'NextToken' in res:
    res = printPartition(res['NextToken'])


print('Partition list Completed')


