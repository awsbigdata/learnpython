import boto3

import datetime

client = boto3.client('cloudwatch')

response = client.list_metrics(Namespace='AWS/DynamoDB',MetricName='ProvisionedWriteCapacityUnits',Dimensions=[
        {
            'Name': 'TableName',
            'Value': 'autoscale_test'
        },
    ])

print(str(response))

a = datetime.datetime(2017, 11, 8,10,11,12)

for i in range(1,100):
    b = a + datetime.timedelta(0, i)
    response = client.put_metric_data(
    Namespace='AWS/DynamoDB',
    MetricData=[
        {
            'MetricName': 'ConsumedReadCapacityUnits',
            'Dimensions': [
                {
                    'Name': 'TableName',
                    'Value': 'autoscale_test'
                },
            ],
            'Timestamp': b,

            'StatisticValues': {
                'Sum': 123.0,
                'SampleCount':12.0,
                'Minimum':14.0,
                'Maximum':17.0
            },
            'Unit': 'Seconds',
         },
    ]
        )
    print("run " +str(i))

print(str(response))

