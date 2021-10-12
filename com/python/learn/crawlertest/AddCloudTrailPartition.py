import boto3
import datetime
import time


a = datetime.datetime(2013,4,6)

region='us-east-1'
table_bucket='awssrramascloudtrail'
table_prefix='AWSLogs/898623153764/CloudTrail'

stage_loc='s3://testeastreg/output'

database='db'


client = boto3.client('athena',region_name=region)
s3_client = boto3.client('s3',region_name=region)


database='db'

def path_exist(prefix):
    response = s3_client.list_objects_v2(
        Bucket=table_bucket,
        MaxKeys=1,
        Prefix=prefix
    )
    return response['KeyCount']>0

for i in range(1,1000):
    b = a + datetime.timedelta(i)
    path_prefix=table_prefix+'/'+region+'/'+str(b.year)+"/"+str('{:02d}'.format(b.month))+"/"+str('{:02d}'.format(b.day))+"/"
    if(True):
        query="ALTER TABLE cloudtrail ADD if not exists PARTITION (region='us-east-1',year='"+str(b.year)+"',month='"+str('{:02d}'.format(b.month))+"',day='"+str('{:02d}'.format(b.day))+"') " \
             "LOCATION 's3://"+table_bucket+'/'+table_prefix+'/'+region+'/'+str(b.year)+"/"+str('{:02d}'.format(b.month))+"/"+str('{:02d}'.format(b.day))+"/'"
        print(query)
        res = client.start_query_execution(QueryString=query, QueryExecutionContext={'Database': database},
                                   ResultConfiguration={'OutputLocation':stage_loc })
        print(res)
    else:
        print("path does not exist : s3://"+table_bucket+'/'+path_prefix)
    #time.sleep(1)


print("completed")


