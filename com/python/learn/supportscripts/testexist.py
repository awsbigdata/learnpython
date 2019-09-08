import boto3

import sys

glue = boto3.client('glue')
emr_client = boto3.client('emr')

dbname="hive_glue"
table_name="alb_logs"
#response = glue.get_table(DatabaseName=dbname,Name=table_name)

print(boto3.__version__)

describe_cluster_response = emr_client.list_instance_fleets(ClusterId='j-2K1NX5IJ5Z90' )
#print(describe_cluster_response)

for ciusterinfocluster in describe_cluster_response["InstanceFleets"]:
    #print(ciusterinfocluster)

    targetondemandCapacity = ciusterinfocluster['TargetOnDemandCapacity']
    targetSpotCapacity = ciusterinfocluster['TargetSpotCapacity']
    instance_type = ciusterinfocluster['InstanceFleetType']
    print(targetondemandCapacity)
    print(targetSpotCapacity)

    if (targetondemandCapacity > 0):
        provisionedOnDemandCapacity = ciusterinfocluster['ProvisionedOnDemandCapacity']

        print(provisionedOnDemandCapacity)


        if (provisionedOnDemandCapacity != targetondemandCapacity):
            print(" ProvisionedOnDemandCapacity is not same as target for {}".format(instance_type))        #self.send_email(self.getEmailAddress(resultsid),
                      #  "EMR cluster instance requested is not equla to what AWS emr provided please check the your cluster")

    if (targetSpotCapacity > 0):
        provisionedSpotCapacity = ciusterinfocluster['ProvisionedSpotCapacity']
        print(provisionedSpotCapacity)
        if (provisionedSpotCapacity != targetSpotCapacity):
            print(" ProvisionedSpotCapacity is not same as target for {}".format(instance_type))
        #self.send_email(self.getEmailAddress(resultsid),
         #               "EMR cluster instance requested is not equla to what AWS emr provided please check the your cluster")