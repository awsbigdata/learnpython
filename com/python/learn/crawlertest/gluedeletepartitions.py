import boto3


region='us-east-1'
client = boto3.client('glue',region_name=region)
databaseName=""
tableName=""


response = client.delete_partition(
    DatabaseName=databaseName,
    TableName=tableName,
    PartitionValues=[
        '2019','9','6','11'
    ]
)
