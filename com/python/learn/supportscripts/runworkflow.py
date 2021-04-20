import boto3

client = boto3.client('glue')

def runwf(path):
    response = client.update_workflow(
        Name='workflowetl',
        DefaultRunProperties={
            'target_s3_location': path
        }
    )
    response = client.start_workflow_run(
        Name='workflowetl'
    )


response = client.get_table(
    DatabaseName='default',
    Name='learn_lu'
)

print(response)
