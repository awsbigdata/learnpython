#!/usr/bin/python

#
# This program used to sync the glue table schema and partition upto 1000
#
import boto3
from sys import argv
import json
from datetime import date, datetime

dbname='hive_glue'
tablename='bm_cities'


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

res=client.get_partitions(DatabaseName=dbname,TableName=tablename,MaxResults=1000)

print('Total partitions',len(res['Partitions']))

print('Partition sync started')

for epart in res['Partitions']:
    epart.pop('DatabaseName','None')
    epart.pop('CreationTime','None')
    epart.pop('TableName', 'None')
    epart['StorageDescriptor']['Columns']=storagedes['Columns']
    print(json.dumps(epart['Values']))
    print(json.dumps(epart,default=json_serial))
   #response = client.update_partition(DatabaseName=dbname, TableName=tablename,
                                  #     PartitionValueList=epart['Values'], PartitionInput=epart)

print('Partition sync Completed')


tz=""


def map_function(dynamicRecord):
    dynamicRecord['header__operation']='Insert'
    dynamicRecord['year']=datetime.now().strftime("%Y")
    dynamicRecord['month']=datetime.now().strftime("%m")
    dynamicRecord['day']=datetime.now().strftime("%d")
    dynamicRecord['hour']=datetime.now().strftime("%H")
    return dynamicRecord

partition_info=json.loads('{"StorageDescriptor":{"OutputFormat":"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat","InputFormat":"org.apache.hadoop.mapred.TextInputFormat","SerdeInfo":{"SerializationLibrary":"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe","Parameters":{"field.delim":","}},"Parameters":{"classification":"csv","recordCount":"2","typeOfData":"file","delimiter":",","skip.header.line.count":"1"},"Location":"s3://athenaiad/cities/year=2010/month=12/day=11/","Columns":[{"Type":"bigint","Name":"id"},{"Type":"string","Name":"name"}],"Compressed":false},"Values":["2010","12","11"]}')

response = client.batch_create_partition(DatabaseName=dbname,TableName=tablename, PartitionInputList=[partition_info])
print(response)