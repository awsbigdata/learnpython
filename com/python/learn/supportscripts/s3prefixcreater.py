import boto3

# Create a client
client = boto3.client('s3', region_name='us-west-2')

# Create a reusable Paginator
paginator = client.get_paginator('list_objects')

bucket_name='athenaiad'
# Create a PageIterator from the Paginator
page_iterator = paginator.paginate(Bucket=bucket_name)

file = open('../../../../s3prefixfile.txt', 'wb')
count=0
target=133581
for page in page_iterator:
    for eprefix in page['Contents']:
        print("s3://{}{}".format(bucket_name,eprefix['Key']))
        file.write("s3://{}{}".format(bucket_name,eprefix['Key']))
        count=count+1
        if count >target:
            break
    if count > target:
        break

file.close()