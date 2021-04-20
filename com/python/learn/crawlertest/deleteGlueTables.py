import boto3

glue = boto3.client('glue',region_name='us-east-1')

database="lakeglue"

response = glue.get_tables(
    DatabaseName=database,

)

for table in response['TableList']:
    #print(table['Name'])
    if table['Name'] in "metadata":
        glue.delete_table(
            DatabaseName=database,
            Name=table['Name']
        )
        print("deleting table {}".format(table['Name']))