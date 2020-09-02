
#
#  Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  This file is licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License. A copy of
#  the License is located at
#
#  http://aws.amazon.com/apache2.0/
#
#  This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
#  CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
#
from __future__ import print_function # Python 2/3 compatibility
import boto3

from sys import argv
from boto3.dynamodb.conditions import Key, Attr


dbname=argv[1]
TTL_hours=argv[2]

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table(dbname)

from datetime import datetime
from datetime import timedelta

## Please run it on AWS machine as System time should be in UTC

d = datetime.today() - timedelta(hours=int(TTL_hours))

epoch_time=int(d.strftime('%s'))*1000

##print(epoch_time)

fe = Attr('lastModified').lt(epoch_time) & ~Key('hashKey').eq('MultiKeyStoreTag')

pe = "hashKey,rangeKey,lastModified"

response = table.scan(
    FilterExpression=fe,
     ProjectionExpression = pe
    )


def deleteOldkeys(response):
    with table.batch_writer(overwrite_by_pkeys=[]) as batch:
            for i in response['Items']:
                batch.delete_item(
                    Key={
                        'hashKey': i['hashKey'],
                        'rangeKey': i['rangeKey']
                    })

    print('batch delete completed')




deleteOldkeys(response)

while 'LastEvaluatedKey' in response:
    response = table.scan(
        FilterExpression=fe,
        ProjectionExpression=pe,
        ExclusiveStartKey=response['LastEvaluatedKey']
        )
    deleteOldkeys(response)

