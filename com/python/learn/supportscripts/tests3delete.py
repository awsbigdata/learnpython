#!/usr/bin/env python
import boto3

client = boto3.client('s3')

bucketname='aversiontest'
prefix='defaultssetest'


def deleteoldobjBatch(response):
    batch_size = 10
    prefixes = []
    index = 0
    if 'Versions' in response:
        print("Old Version might present")
        for eacho in response['Versions']:
            if(eacho['IsLatest']==False):
                index = index + 1
                prefix = {"Key": eacho['Key'], "VersionId": eacho['VersionId']}
                prefixes.append(prefix)
            if (index % batch_size):
                print(prefixes)
                response = client.delete_objects(
                    Bucket=bucketname, Delete={'Objects': prefixes})
                print("deleted : " + str(response))
                prefixes = []
                index = 0
        if (len(prefixes) > 0):
            response = client.delete_objects(
                Bucket=bucketname, Delete={'Objects': prefixes})


def deleteVersionbatch(response):
    batch_size = 10
    prefixes = []
    index = 0
    if 'DeleteMarkers' in response:
        print("Delete Markers present")
        for eacho in response['DeleteMarkers']:
            index = index + 1
            prefix = {"Key": eacho['Key'], "VersionId": eacho['VersionId']}
            prefixes.append(prefix)
            if (index % batch_size):
                print(prefixes)
                response = client.delete_objects(
                    Bucket=bucketname, Delete={'Objects': prefixes})
                print("deleted : " + str(response))
                prefixes = []
                index = 0
        if (len(prefixes) > 0):
            response = client.delete_objects(
                Bucket=bucketname, Delete={'Objects': prefixes})


while True:
    response = client.list_object_versions(Bucket=bucketname,Prefix=prefix)
    #print(response)
    deleteoldobjBatch(response)
    deleteVersionbatch(response)
    if(response['IsTruncated']==False):
        break


