#!/usr/bin/python

#
# This program used to create or update the partition in Glue
#

import boto3
import json

dbname='hive_glue'
tablename='json_addpart'

client = boto3.client('glue',region_name='us-east-1')


dicts=json.loads('{"StorageDescriptor": {"OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat", "SortColumns": [], "InputFormat": "org.apache.hadoop.mapred.TextInputFormat", "SerdeInfo": {"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe", "Parameters": {"paths": "name,stdate"}}, "BucketColumns": [], "Parameters": {"compressionType": "none", "classification": "json", "recordCount": "5", "typeOfData": "file", "objectCount": "1", "averageRecordSize": "56", "sizeKey": "283"}, "Location": "s3://athenaiad/json/2016/05/16/15/", "NumberOfBuckets": -1, "StoredAsSubDirectories": false, "Columns": [{"Type": "string", "Name": "name"}, {"Type": "string", "Name": "stdate"}], "Compressed": false}, "Parameters": {}, "Values": ["2016", "05", "18", "15"]}');

response = client.create_partition(DatabaseName=dbname, TableName=tablename,PartitionInput=dicts)

pv=["2016", "05", "18", "15"]
pvl=json.loads('{"StorageDescriptor": {"OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat", "SortColumns": [], "InputFormat": "org.apache.hadoop.mapred.TextInputFormat", "SerdeInfo": {"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe", "Parameters": {"paths": "name,stdate"}}, "BucketColumns": [], "Parameters": {"compressionType": "none", "classification": "json", "recordCount": "5", "typeOfData": "file", "objectCount": "1", "averageRecordSize": "56", "sizeKey": "283"}, "Location": "s3://athenaiad/json/2016/05/16/15/", "NumberOfBuckets": -1, "StoredAsSubDirectories": false, "Columns": [{"Type": "string", "Name": "name"}, {"Type": "string", "Name": "stdate"}], "Compressed": false}, "Parameters": {}, "Values": ["2016", "05", "18", "15"]}')

response = client.update_partition(DatabaseName=dbname, TableName=tablename,PartitionValueList=pv, PartitionInput=pvl)
print(response)
print('Partition sync Completed')