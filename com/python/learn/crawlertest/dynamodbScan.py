import boto3
client = boto3.client('dynamodb', region_name='us-east-1')
paginator = client.get_paginator('scan')
page_iterator = paginator.paginate(
    TableName='earthdata',
    PaginationConfig={
        'MaxItems': 1,
        "PageSize": 1,
    }
)

for page in page_iterator:
    print(page)

print(boto3.__version__)