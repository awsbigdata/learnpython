import boto3

glue = boto3.client('glue')


response = glue.get_table(

    DatabaseName='hive_glue',
    Name='testvb'
)

print(response)