import boto3

import sys

glue = boto3.client('glue')

response = glue.get_classifier(
    Name='csvtac'
)

print(response)