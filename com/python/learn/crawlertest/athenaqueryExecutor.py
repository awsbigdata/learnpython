#!/usr/bin/python

import boto3
from multiprocessing import Process, Lock

import time
client = boto3.client('athena',region_name='us-east-1')

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
    query = """SELECT * FROM athena_learn.akafkatest limit 10;"""
    print(query)
    res = client.start_query_execution(QueryString=query, QueryExecutionContext={'Database': 'grab_incentives'},

                                       ResultConfiguration={'OutputLocation': 's3://athenaiad/output'})
    print(res)
    queryid = res['QueryExecutionId']
    response = client.get_query_execution(
        QueryExecutionId=queryid
    )
    status = response['QueryExecution']['Status']['State']
    print(status)
    while (status != "SUCCEEDED" and status != "FAILED"):
        ##time.sleep(10)
        response = client.get_query_execution(
            QueryExecutionId=queryid
        )
        #print(response)
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


if __name__ == '__main__':
    lock = Lock()

    for num in range(10):
        Process(target=athena_select).start()


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


