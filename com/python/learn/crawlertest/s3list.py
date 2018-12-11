import boto3

s3_client = boto3.client('s3')

res=s3_client.list_objects(Bucket='awssrramascloudtrail',Prefix='AWSLogs/898623153764/CloudTrail')

print(res)

response = s3_client.list_objects_v2(
    Bucket='awssrramascloudtrail',
    MaxKeys=1,
    Prefix='AWSLogs/898623153764/CloudTrail'
)

file_exists = response['KeyCount']

print(file_exists)