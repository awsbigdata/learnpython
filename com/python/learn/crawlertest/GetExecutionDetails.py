import boto3



queryExecutionIds=['fe35729c-fbba-4089-9838-79dcdc679f90','194b4b8e-c0cd-4139-916f-eda38608fa4c','e9007f1d-c0b2-4604-af59-a536a1e66531']



def getDatascan(queryids):
    ldatascan=[]
    client = boto3.client('athena',region_name='us-east-1')
    while queryids:
        try:
            res = client.batch_get_query_execution(QueryExecutionIds=queryids)
            for queryexecution in res['QueryExecutions']:
                datascan = {}
                datascan['queryexecutionid']=queryexecution['QueryExecutionId']
                datascan['datascan']=queryexecution['Statistics']['DataScannedInBytes']
                datascan['executiontime'] =queryexecution['Statistics']['EngineExecutionTimeInMillis']
                ldatascan.append(datascan)
            del queryids[:]
            if res['UnprocessedQueryExecutionIds']:
                for unprocessqid in res['UnprocessedQueryExecutionIds']:
                    queryids.append(unprocessqid['QueryExecutionId'])
        except:
            print("error")
    return ldatascan

print(getDatascan(queryExecutionIds))