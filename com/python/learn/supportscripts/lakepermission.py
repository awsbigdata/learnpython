import boto3

client = boto3.client('lakeformation')

response = client.grant_permissions(
    Principal={
        'DataLakePrincipalIdentifier': 'arn:aws:iam::898623153764:role/Admin'
    },
    Resource={
        'Table': {
            'DatabaseName': 'hive_glue',
            'TableWildcard': {}
        }
    },
    Permissions=[
       'SELECT','INSERT','DROP'
    ]
)

print(response)