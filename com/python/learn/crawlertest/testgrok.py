
import time


strd="%{LOGLEVEL:col},"
col=""
for i in range(0,140):
    col+=strd

#print(col)

import boto3
import time

client = boto3.client('s3',region_name='us-east-1')

response = client.list_objects_v2(
    Bucket='athenaiad',
    Delimiter='/',
    Prefix='athenalab/'
)
print(response)


import boto3
import time
glue = boto3.client('glue')

def deleteCrawler(name):
    response = glue.get_crawlers()
    res='none'
    for cname in response['Crawlers']:
        if cname['Name'] == name:
            print(cname['State'])
            while cname['State']!='READY':
                if(cname['State']=='RUNNING'):
                    response = glue.stop_crawler(Name=cname['Name'])
                time.sleep(5)
                cres=glue.get_crawler(Name=cname['Name'])
                cname=cres['Crawler']
            res = glue.delete_crawler(Name=name)
            print(res)
    return res
s3 = boto3.resource('s3')

for i in s3.buckets.all():
    print(i.name)
#deleteCrawler('Athena_labq1')