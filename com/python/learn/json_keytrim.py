import boto3
import json

glue = boto3.client('glue',region_name='us-east-1')

ddl=''
for i in open('/home/local/ANT/srramas/Downloads/testcreatetable.txt','r'):
    ddl=ddl+i



ddl_json=json.loads(ddl)

for i in ddl_json['TableList']:
    del i['PartitionKeys']
    i['PartitionKeys']=[]
    i['StorageDescriptor']['Location']='s3://athenaiad/a5121390841/'
    response = glue.create_table(DatabaseName='testjson',TableInput=i
                                 )
    print(i)