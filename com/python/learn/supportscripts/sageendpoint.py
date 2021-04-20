import boto3
client = boto3.client('sagemaker')

# create sagemaker model
create_model_api_response = client.create_model(
    ModelName='pytorchserve-test11',
    PrimaryContainer={
    "Image": "763104351884.dkr.ecr.us-east-1.amazonaws.com/pytorch-inference:1.6.0-cpu-py3",
    "ModelDataUrl": "s3://athenaiad/sageserv/pytorchserv.tar.gz",
    "Environment": {
    "SAGEMAKER_PROGRAM": "torchserve-predictor.py",
    "SAGEMAKER_SUBMIT_DIRECTORY": "s3://athenaiad/sageserv/pytorchserv.tar.gz",
    "SAGEMAKER_CONTAINER_LOG_LEVEL": "20",
    "SAGEMAKER_REGION": "us-east-1",
    "MMS_DEFAULT_RESPONSE_TIMEOUT": "500"}
},
    ExecutionRoleArn='arn:aws:iam::898623153764:role/testsg'
)

print ("create_model API response", create_model_api_response)

# create sagemaker endpoint config
create_endpoint_config_api_response = client.create_endpoint_config(
    EndpointConfigName='mysageendpoint-test11',
    ProductionVariants=[
    {
        "VariantName": "AllTraffic",
        "ModelName": "pytorchserve-test11",
        "InitialInstanceCount": 1 ,
        "InstanceType": "ml.m4.xlarge",
        "InitialVariantWeight": 1.0
    }
]
)

print ("create_endpoint_config API response", create_endpoint_config_api_response)

# create sagemaker endpoint
create_endpoint_api_response = client.create_endpoint(
    EndpointName='mysageendpointhost-test11',
    EndpointConfigName='mysageendpoint-test11',
)



print ("create_endpoint API response", create_endpoint_api_response)