import boto3
import datetime
import time

client = boto3.client('emr')
endtime=datetime.datetime.now()
start_date = datetime.datetime.now() + datetime.timedelta(-30)
response = client.list_clusters(
    CreatedAfter=start_date,
    CreatedBefore=endtime
)

for cluster in response['Clusters']:
    time.sleep(1)
    response = client.list_instances( ClusterId=cluster['Id'])
    for instance in response['Instances']:
        # cluster id,cluster name,PublicDnsName,PublicIpAddress,PrivateDnsName,PrivateIpAddress
        print('{0},{1},{2},{3},{4},{5},{6}'.format(cluster['Id'],cluster['Name'],instance['Ec2InstanceId'],instance['PublicDnsName'],instance['PublicIpAddress'],instance['PrivateDnsName'],instance['PrivateIpAddress']))

