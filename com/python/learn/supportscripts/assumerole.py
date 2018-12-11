import boto3

#use lambda role to create a sts
lclient = boto3.client('sts')

#assume the role
response = lclient.assume_role(RoleArn="arn:aws:iam::236475725625:role/ProjectOneCallQualityAccess",RoleSessionName="AthenaQueryRun")

##use the temp credential
session=boto3.Session(
        aws_access_key_id=response['Credentials']['AccessKeyId'],
        aws_secret_access_key=response['Credentials']['SecretAccessKey'],
        aws_session_token=response['Credentials']['SessionToken'])

#create Athena instance for assumed role
client=session.client('athena',region_name='us-east-1')

