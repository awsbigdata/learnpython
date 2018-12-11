import boto3
import json

datapipeline_client = boto3.client('datapipeline',region_name='us-east-1')
pipelineId='df-07575631T1Z9SV9TEGOA'

output_file="/tmp/"+pipelineId+".json"

output = open(output_file, "w")
json_dict=datapipeline_client.get_pipeline_definition(pipelineId=pipelineId)

json.dump(json_dict, output)

print("find the json here")
print(output_file)