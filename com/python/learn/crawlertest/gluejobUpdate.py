
import boto3



client = boto3.client('glue',region_name='us-east-1')

response = client.update_job(JobName='test', JobUpdate={'Command': {
            'Name': 'glueetl',
            'ScriptLocation': 's3://aws-glue-scripts-898623153764-us-east-1/admin/test'
        },'Role':'arn:aws:iam::898623153764:role/AWSGlueServiceRoleDefault','Timeout': 123,
        'NotificationProperty': {
            'NotifyDelayAfter': 123
        }})

print(response)