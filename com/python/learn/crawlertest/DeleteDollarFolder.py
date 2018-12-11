import boto3;




def tempFolderdeletion(bucketname,outputprefix):
    client = boto3.client('s3')
    response = client.list_objects(
                Bucket=bucketname,
                Delimiter='/',
                Prefix=outputprefix
                )

    print("Dollar temp folder deletion has started")
    print(response)
    for ekey in response['Contents']:
        if '$' in ekey['Key']:
            client.delete_object(
                Bucket=bucketname,
                Key=ekey['Key']
                )
            print(ekey['Key'] + " Folder has been deleted")

    print("Dollar temp folder deletion has been Completed")


def tempFileDeletion(bucketname,outputprefix):
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucketname,Prefix=outputprefix)
    delete_us = dict(Objects=[])
    for item in pages.search('Contents'):
        if "$" in item['Key']:
           delete_us['Objects'].append(dict(Key=item['Key']))
        if len(delete_us['Objects']) >= 1000:
            client.delete_objects(Bucket=bucketname, Delete=delete_us)
            print(delete_us)
            delete_us = dict(Objects=[])
    # flush once aws limit reached
    if len(delete_us['Objects']):
        client.delete_objects(Bucket=bucketname, Delete=delete_us)
        print(delete_us)


tempFileDeletion('athenaiad','temp/')