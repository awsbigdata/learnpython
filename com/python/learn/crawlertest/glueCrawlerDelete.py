import boto3
import json
from datetime import date, datetime

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()


client = boto3.client('glue',region_name='us-east-1')

response = client.get_crawlers(MaxResults=100)

print(response)
response = client.get_crawler(Name='lab1')
print(response['Crawler'])

print(json.dumps(response['Crawler'],default=json_serial))

str='{"Targets":{"JdbcTargets": [], "S3Targets": [{"Path": "s3://athenaiad/athenalab/exe1", "Exclusions": []}], "DynamoDBTargets": []},  "Role": "AWSGlueServiceRoleDefault", "DatabaseName": "hive_glue", "SchemaChangePolicy": {"DeleteBehavior": "DEPRECATE_IN_DATABASE", "UpdateBehavior": "UPDATE_IN_DATABASE"}, "TablePrefix": "lab_", "Classifiers": []}'
crawlersetup=json.loads(str)
response = client.create_crawler(Name='lab12',Role=crawlersetup['Role'],
                              DatabaseName=crawlersetup['DatabaseName'],Targets=crawlersetup['Targets'],TablePrefix=crawlersetup['TablePrefix'])


print(response)
'''
for cname in response['Crawlers']:
    response = client.delete_crawler(Name=cname['Name'])
    print(response)

response = client.get_classifiers(MaxResults=100)
print(response)
for gname in response['Classifiers']:
    response = client.delete_classifier(Name=gname['GrokClassifier']['Name'])
    print(response)
'''