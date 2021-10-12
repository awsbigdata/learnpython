import boto3
ssm = boto3.client('ssm',region_name='us-east-1')
parameter = ssm.get_parameter(Name='/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2', WithDecryption=True)
parameter['Parameter']['Value']