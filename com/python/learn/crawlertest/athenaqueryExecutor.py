#!/usr/bin/python

import boto3
import time
from multiprocessing import Process, Lock

client = boto3.client('athena',region_name='us-east-1')
gluecli = boto3.client('glue', region_name='us-east-1')


def getTables(dbname):
    response = gluecli.get_tables(DatabaseName=dbname)
    tnames=[]
    for table in response['TableList']:
        tnames.append(table['Name'])

    return tnames



def athena_select(dbname,tbname):
    query = """SELECT *  FROM {}.{}  limit 10""".format(dbname,tbname)
    print(query)
    res = client.start_query_execution(QueryString=query, QueryExecutionContext={'Database': dbname},

                                       ResultConfiguration={'OutputLocation': 's3://athenaiad/output'})
    print(res)
    queryid = res['QueryExecutionId']
    response = client.get_query_execution(
        QueryExecutionId=queryid
    )
    status = response['QueryExecution']['Status']['State']
    print(status)
    while (status != "SUCCEEDED" and status != "FAILED"):
        time.sleep(5)
        response = client.get_query_execution(
            QueryExecutionId=queryid
        )
        #print(response)
        status = response['QueryExecution']['Status']['State']
    if(status =='FAILED'):
        print(response['QueryExecution']['Status']['StateChangeReason'])
        print("query id : "+queryid)
    return queryid,status

def deleteTable(dbname,tablename):
    gluecli.delete_table(DatabaseName=dbname,Name=tablename)


response = gluecli.get_databases()
for db in response['DatabaseList']:
    dbname=db['Name']
    print("dbname ",dbname)
    tables=getTables(dbname)
    for table in tables:
        queryid,status=athena_select(dbname,table)
        if(status=='SUCCEEDED'):
            response = client.get_query_results(QueryExecutionId=queryid)
            if (len(response['ResultSet']['Rows'])<2):
                print("deleting table : ",table)
                deleteTable(dbname,table)
        else:
            print("deleting table :", table)
            deleteTable(dbname, table)


print("Execution completed")




