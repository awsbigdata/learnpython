import paramiko

import boto3

s3_client = boto3.client('glue')
res = s3_client.get_connection(Name='mssqlec2')
print(res)
del res['Connection']['CreationTime']
del res['Connection']['LastUpdatedTime']
#del res['Connection']['LastUpdatedBy']
res['Connection']['ConnectionProperties']['JDBC_ENFORCE_SSL']='true'
res['Connection']['ConnectionProperties']['Encrypt']='true'
res['Connection']['ConnectionProperties']['TrustServerCertificate']='true'
response = s3_client.update_connection(Name='mssqlec2',ConnectionInput=res['Connection'])
print(response)