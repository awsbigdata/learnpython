import boto3
import time

client = boto3.client('emr')
cid='j-xxxxxxx'
sname="testJar"


def cancel_resubmit(clusterid,stepname):
    """
    Cancel the running steps based on step name and
    resubmit the same step from one of the sampling
    """
    step_config = {'Name': '',
                   'HadoopJarStep': {'Jar': 's3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',
                                     'Args': []}}
    step_config['Name'] = stepname
    response = client.list_steps(
        ClusterId=clusterid,
        StepStates=[
            'RUNNING', 'FAILED'
        ]
    )
    for step in response['Steps']:
        print(step)
        if (step['Name'] == stepname):
            step_config['HadoopJarStep']['Args'] = step['Config']['Args']
            if (step['Status']['State'] == 'RUNNING'):
                response = client.cancel_steps(
                    ClusterId=clusterid,
                    StepIds=[step['Id']],
                    StepCancellationOption='TERMINATE_PROCESS'
                )
                print(response)
            time.sleep(10)
        print("step cancelled")
    print(step_config)
    print("adding steps")
    response = client.add_job_flow_steps(JobFlowId=clusterid, Steps=[step_config])
    print(response)

cancel_resubmit(cid,sname)