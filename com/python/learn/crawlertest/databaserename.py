#!/usr/bin/python

#
# This program used to sync the glue table schema and partition upto 1000
#
import boto3
from sys import argv

#dbname='hive_glue'
#tablename='test'


if(len(argv) !=3):
    print("please pass the database and table name")
    exit(-1)

dbname=argv[1]
tablename=argv[2]

client = boto3.client('glue',region_name='us-east-1')

response = client.get_table(
    DatabaseName=dbname,
    Name=tablename
)

storagedes=response['Table']['StorageDescriptor']

res=client.get_partitions(DatabaseName=dbname,TableName=tablename,MaxResults=1000)

print('Total partitions',len(res['Partitions']))

print('Partition sync started')

for epart in res['Partitions']:
    epart.pop('DatabaseName','None')
    epart.pop('CreationTime','None')
    epart.pop('TableName', 'None')
    epart['StorageDescriptor']['Columns']=storagedes['Columns']
    response = client.update_partition(DatabaseName=dbname, TableName=tablename,
                                       PartitionValueList=epart['Values'], PartitionInput=epart)

print('Partition sync Completed')