#!/usr/bin/env python
import boto3
import json
from datetime import datetime,date

epoch = datetime.utcfromtimestamp(0)

def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return str(obj.utcnow().replace(microsecond=0).isoformat())+"Z"
#dbname=argv[1]
#tablename=argv[2]

client = boto3.client('sts')
credentials = client.get_session_token()
credentials['Credentials']['Version']=1
print(json.dumps(credentials['Credentials'],default=json_serial))

