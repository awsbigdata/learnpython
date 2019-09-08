#!/usr/bin/python

import boto3
import time

client = boto3.client('athena',region_name='us-east-1')

query="select * from store_sales limit 100000"
print(query)
for i in range(1,30):
    res = client.start_query_execution(QueryString=query, QueryExecutionContext={'Database': 'hive_glue'},ResultConfiguration={'OutputLocation':'s3://athenaiad/output'})

print(res)
queryid=res['QueryExecutionId']
response = client.get_query_execution(
    QueryExecutionId=queryid
)
status=response['QueryExecution']['Status']['State']
print(status)
while (status == "RUNNING"):
    time.sleep(10)
    response = client.get_query_execution(
    QueryExecutionId=queryid
      )
    status = response['QueryExecution']['Status']['State']
    print(status)


print("Execution completed")

response = client.get_query_results(QueryExecutionId=queryid)

def processRow(row,columnInfo):
    for meta in columnInfo:
       print(meta['Type'])






