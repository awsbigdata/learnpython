import boto3

client = boto3.client('emr')

response = client.list_clusters(
       ClusterStates=[
        'RUNNING' , 'WAITING'
    ]
)

for cluster in response['Clusters']:
    if cluster['Name']=='My cluster':
        print("terminating : ",cluster['Id'])
        response = client.terminate_job_flows(JobFlowIds=[cluster['Id']])
        print("terminated the cluster : ",response)

print(response)