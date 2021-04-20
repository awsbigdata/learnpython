import boto3

glue = boto3.client('glue')

response = glue.get_workflow(
    Name='workflowetl',
    IncludeGraph=True
)
print(response)
response = glue.resume_workflow_run(
Name='workflowetl',
RunId='wr_6a63d24ba61d4a67bffde719da30cfe5e06f6c47429782d97e14e74228787c89',
NodeIds=['wnode_98065cfa4e23ccdfe2b1b317d12d08505a12af320d6f7ac86da845bec9a2b722']
)
print(response)
##s3://athenaiad/glueboto3/boto3-1.17.54-py2.py3-none-any.whl