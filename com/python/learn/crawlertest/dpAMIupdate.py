#!/usr/bin/python

import boto3
import json

datapipeline_client = boto3.client('datapipeline',region_name='us-east-1')
pipelineId='df-03531331U7NXLMPUC7QZ'
amiid='ami-03fc40c0bc4f73c87'


json_dict=datapipeline_client.get_pipeline_definition(pipelineId=pipelineId)

for obj in json_dict['pipelineObjects']:
    for key in obj['fields']:
        if key['key'] =='imageId':
            key['stringValue']=amiid


print(json.dumps(json_dict))
out=datapipeline_client.put_pipeline_definition(pipelineId=pipelineId,pipelineObjects=json_dict['pipelineObjects'],parameterObjects=json_dict['parameterObjects'],parameterValues=json_dict['parameterValues'])
print(out)
response = datapipeline_client.activate_pipeline(pipelineId=pipelineId)
print(response)