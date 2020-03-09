import boto3

glue = boto3.client('glue')
res = glue.get_connection(Name='mssqlec2')

username=res['Connection']['ConnectionProperties']['USERNAME']
password=res['Connection']['ConnectionProperties']['PASSWORD']
JDBC_URL=res['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
##+61432869969