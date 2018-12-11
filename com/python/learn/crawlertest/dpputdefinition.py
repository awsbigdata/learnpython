import boto3
import json

import psycopg2;


s3 = boto3.client('s3')



#s3 path s3://athena-organ-test/testdp/output.json

bucket='athena-organ-test'
key='testdp/output.json'

obj = s3.get_object(Bucket=bucket, Key=key)


#print(obj)
res_json = obj['Body'].read().decode("utf-8")
#print(res_json)
json_dict= json.loads(str(res_json))
datapipeline_client = boto3.client('datapipeline',region_name='us-east-1')
pipelineId='df-06915561Q6WC8Y41CES5'

#df=datapipeline_client.create_pipeline(name='test4',uniqueId='sdfsdasdf');
#print(str(df))
output_file="/home/local/ANT/srramas/Downloads/output.json"

output = open(output_file, "w")

json_dict=datapipeline_client.get_pipeline_definition(pipelineId='df-02435503EHY6RBVL5XWL')

json.dump(json_dict, output)

#out=datapipeline_client.put_pipeline_definition(pipelineId=pipelineId,pipelineObjects=json_dict['pipelineObjects'],parameterObjects=json_dict['parameterObjects'],parameterValues=json_dict['parameterValues'])

