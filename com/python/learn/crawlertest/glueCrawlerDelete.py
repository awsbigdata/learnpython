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

for cname in response['Crawlers']:
    response = client.delete_crawler(Name=cname['Name'])
    print(response)

response = client.get_classifiers(MaxResults=100)
print(response)
for gname in response['Classifiers']:
    response = client.delete_classifier(Name=gname['GrokClassifier']['Name'])
    print(response)
