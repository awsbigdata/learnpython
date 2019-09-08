#!/usr/bin/python

#
# This program used to sync the glue table schema and partition upto 1000
#
import boto3
from sys import argv
import time

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

count=0

def sync_partition_batch(next_token=None):
    if next_token is None:
        res = client.get_partitions(DatabaseName=dbname, TableName=tablename, MaxResults=500)
    else:
        res = client.get_partitions(DatabaseName=dbname, TableName=tablename, MaxResults=500,NextToken=next_token)
    count=len(res['Partitions'])
    print('Total batch partitions', len(res['Partitions']))
    print('Partition sync started')
    if("Partitions" in res):
        for epart in res['Partitions']:
            epart.pop('DatabaseName', 'None')
            epart.pop('CreationTime', 'None')
            epart.pop('TableName', 'None')
            epart['StorageDescriptor']['Columns'] = storagedes['Columns']
            client.update_partition(DatabaseName=dbname, TableName=tablename,
                                               PartitionValueList=epart['Values'], PartitionInput=epart)
    return res,count


res,cnt=sync_partition_batch()
count=count+cnt

while('NextToken' in res.keys() and res['NextToken'] is not None):
     print("Next batch")
     time.sleep(3)
     res,cnt=sync_partition_batch(next_token=res['NextToken'])
     count = count + cnt

print("total number of partition synced {}".format(count))
print('Partition sync Completed')