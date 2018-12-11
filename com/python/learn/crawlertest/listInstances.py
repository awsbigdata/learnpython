import boto3
import json
import pprint
pricing = boto3.client('ec2',region_name='us-east-2')

# To get possible values in the pricing API:

response = pricing.describe_services(ServiceCode='AmazonEC2')
attrs = response['Services'][0]['AttributeNames']
for attr in attrs:
	response = pricing.get_attribute_values(ServiceCode='AmazonEC2', AttributeName=attr)
	values = []
	for attr_value in response['AttributeValues']:
		values.append(attr_value['Value'])
	print("  " + attr + ": " + ", ".join(values))
