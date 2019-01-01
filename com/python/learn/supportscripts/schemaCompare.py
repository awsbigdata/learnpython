import boto3

client=boto3.client("glue")

databasename="testjson"

response = client.get_tables(
    DatabaseName=databasename)
previous=[]
for table in response['TableList']:

    if 'dt_' in table['Name']:

        if previous != table['StorageDescriptor']['Columns']:
            print(table['Name'])
            print("schema : ",previous)
            previous=table['StorageDescriptor']['Columns']


print("completed")


