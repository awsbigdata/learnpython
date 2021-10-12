import boto3

client = boto3.client('athena',region_name='us-east-1')

res = client.start_query_execution(QueryString="CREATE SCHEMA IF NOT EXISTS e6f287f0_ff0a_247ee_86b2_e2acdc49940e", QueryExecutionContext={'Database': "e6f287f0_ff0a_247ee_86b2_e2acdc49940e"},

                                   ResultConfiguration={'OutputLocation': 's3://athenaiad/output'})
print(res)