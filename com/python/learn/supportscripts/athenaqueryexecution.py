#!/usr/bin/python

import boto3
import time

client = boto3.client('athena',region_name='us-east-1')



def athena_select():
    query = """SELECT * FROM "debug_db"."par_testopt" limit 10;"""
    #query=""" SELECT count(1) FROM "athena_training"."testapi" """
    print(query)
    res = client.start_query_execution(QueryString=query, QueryExecutionContext={'Database': 'athena_training'},

                                       ResultConfiguration={'OutputLocation': 's3://athenaiad/output'})

    queryid = res['QueryExecutionId']
    print("Query id : {}".format(queryid))
    response = client.get_query_execution(
        QueryExecutionId=queryid
    )
    status = response['QueryExecution']['Status']['State']
    print(status)
    while (status != "SUCCEEDED" and status != "FAILED"):
       # time.sleep(2)
        response = client.get_query_execution(
            QueryExecutionId=queryid
        )
        status = response['QueryExecution']['Status']['State']
    if(status =='FAILED'):
        print(response['QueryExecution']['Status']['StateChangeReason'])
        print("query id : "+queryid)

    getResults(queryid)
    print("Execution completed")


def getResults(queryid):
    response = client.get_query_results(QueryExecutionId=queryid)
    processRow(response)
    while 'NextToken' in response:
        response = client.get_query_results(QueryExecutionId=queryid, NextToken=response['NextToken'])
        processRow(response)

def processRow(response):
    for row in response['ResultSet']['Rows']:
        for col in row['Data']:
            print(col['VarCharValue'],end=',')
        print('')

athena_select()