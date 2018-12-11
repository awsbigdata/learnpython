import boto3
import json
import datetime
import uuid
import sys
## you can chanage this as

##datapipelineid='df-10008032WH98XHTDEIG3'

datapipelineid = sys.argv[1]

json.loads('{"asd":"fsd"}')

print("cloning dp : "+datapipelineid)
datapipeline_client = boto3.client('datapipeline',region_name='us-east-1')

json_dict=datapipeline_client.get_pipeline_definition(pipelineId=datapipelineid)
response = datapipeline_client.describe_pipelines(pipelineIds=[datapipelineid])

dpname=response['pipelineDescriptionList'][0]['name']

#### schedule change in clone
date = datetime.date.today().strftime('%Y-%m-%d')

for dpobject in json_dict['pipelineObjects']:
    for field in dpobject['fields']:
        if(field['key'] == 'startDateTime'):
            scheduledate=field['stringValue']
            stime=scheduledate.split("T")
            field['stringValue'] ="{}T{}".format(date,stime[1])
        ##print(field)

cresponse = datapipeline_client.create_pipeline(name="{}_pyclone".format(dpname),uniqueId=str(uuid.uuid4()))
newdpid=cresponse['pipelineId']
print("cloned datapipeline id : "+newdpid)
presponse = datapipeline_client.put_pipeline_definition(pipelineId=newdpid,pipelineObjects=json_dict['pipelineObjects'],parameterObjects=json_dict['parameterObjects'],parameterValues=json_dict['parameterValues'])

#Activate the pipeline

response = datapipeline_client.activate_pipeline(pipelineId=newdpid)

print(response)
