#!/usr/bin/python

import boto3
import time
import re
client = boto3.client('athena',region_name='ap-southeast-1')

def deletes3_ob(path):
    s3_client = boto3.client('s3', region_name='us-east-1')
    from urlparse import urlparse
    o = urlparse(path)
    response = s3_client.delete_object(
        Bucket=o.netloc,
        Key=o.path.lstrip('/')    )
    print(response)


def athena_select():
    global queryid, response
    query = """SELECT u.unit_code, u.source, u.vertical, u.executor_type, u.executor_ref, u.unit_ref, u.occurred_at, u.location, u.value, u.subunit, u.subunit_value, u.content, u.metadata FROM grab_incentives.units AS u WHERE ( u.occurred_at >= timestamp '2019-03-05 22:00:00' ) AND ( u.occurred_at < timestamp '2019-03-06 10:01:00' ) AND ( ( ( u.source = 'TRANSPORT' ) AND ( json_extract_scalar(u.metadata, '$.cityID') = '18' OR json_extract_scalar(u.metadata, '$.cityID') IS NULL ) ) OR ( ( u.source = 'LEGACY_TRANSPORT' ) AND ( json_extract_scalar(u.metadata, '$.cityID') = '18' OR json_extract_scalar(u.metadata, '$.cityID') IS NULL ) ) ) /* 63ff33da-c94c-4c70-8893-587ab6675cd6 */"""
    print(query)
    res = client.start_query_execution(QueryString=query, QueryExecutionContext={'Database': 'grab_incentives'},

                                       ResultConfiguration={'OutputLocation': 's3://athenapartitionsync/output'})
    print(res)
    queryid = res['QueryExecutionId']
    response = client.get_query_execution(
        QueryExecutionId=queryid
    )
    status = response['QueryExecution']['Status']['State']
    print(status)
    while (status == "RUNNING"):
        time.sleep(10)
        response = client.get_query_execution(
            QueryExecutionId=queryid
        )
        status = response['QueryExecution']['Status']['State']
    if(status =='FAILED'):
        print(response['QueryExecution']['Status']['StateChangeReason'])
        print("query id : "+queryid)

"""
        m = re.search("s3://[^ ]+", response['QueryExecution']['Status']['StateChangeReason']) 
        if m:
            found = m.group()
            deletes3_ob(found)
            athena_select()
            print(found)
       """


for i in range(1,500):
        time.sleep(100)
        athena_select()



print("Execution completed")

response = client.get_query_results(QueryExecutionId=queryid)

def processRow(row,columnInfo):
    print(row)
    for meta in columnInfo:
       print(meta['Type'])



while 'NextToken' in response:
    response = client.get_query_results(QueryExecutionId=queryid,NextToken=response['NextToken'])
    for row in response['ResultSet']['Rows']:
        processRow(row,response['ResultSet']['ResultSetMetadata']['ColumnInfo'])
    if 'NextToken' in response:
         print(response['NextToken'])


