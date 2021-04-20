import boto3

client = boto3.client('glue')

response = client.get_connection(

    Name='teset'
)
print(response)
response = client.create_connection(
    ConnectionInput={
        'Name': 'mongoapicon',
        'Description': 'mongoapicon',
        'ConnectionType': 'MONGODB',
        'ConnectionProperties': {
            'USERNAME':'mongouser',
            'PASSWORD':'Mongopass',
     'CONNECTION_URL': 'mongodb://mongotestdb.cluster-c9ujozbff8fu.us-east-1.docdb.amazonaws.com:27017',
            'JDBC_ENFORCE_SSL':'true'

},
        'PhysicalConnectionRequirements': {
            'SubnetId': 'subnet-081113b8a4ae6b701',
            'SecurityGroupIdList': [
                'sg-12c9176e','sg-dbe372a6'
            ],'AvailabilityZone':'us-east-1d'
        }
    }
)