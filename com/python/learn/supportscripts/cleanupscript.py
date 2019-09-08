import boto3


class Test(object):
    glue = boto3.client('glue')

    def deleteGlueEndpoint(self):
        response = self.glue.get_dev_endpoints()
        for dev in response['DevEndpoints']:
            res = self.glue.delete_dev_endpoint(EndpointName=dev['EndpointName'])
            print("deleted dev enpoint "+dev['EndpointName'])



    def deleteGlueJobs(self):
        response = self.glue.get_jobs()
        for job in response['Jobs']:
            res=self.glue.delete_job(JobName=job['Name'])
            print("deleting the job",job['Name'])



    def deleteKinesis(self):
        kinesis = boto3.client('kinesis')
        response = kinesis.list_streams()
        for streamName in response['StreamNames']:
            res = kinesis.delete_stream( StreamName=streamName)
            print(res)

    def deleteEMRcluster(self):
        emr = boto3.client('emr')
        response = emr.list_clusters(ClusterStates=['STARTING','BOOTSTRAPPING','RUNNING','WAITING'])
        for cluster in response['Clusters']:
            emr.set_termination_protection(JobFlowIds=[cluster['Id']],TerminationProtected=False)
            res = emr.terminate_job_flows(JobFlowIds=[cluster['Id']])
            print(res)
    def deleteRedshift(self):
        redshift = boto3.client('redshift')
        response = redshift.describe_clusters()
        for cluster in response['Clusters']:
            response = redshift.delete_cluster(ClusterIdentifier=cluster['ClusterIdentifier'],SkipFinalClusterSnapshot=True)
            print(response)

    def cleanUp(self):
       # self.deleteGlueJobs()
        self.deleteGlueEndpoint()
        self.deleteKinesis
        self.deleteEMRcluster()
        self.deleteRedshift()

clean= Test();
clean.deleteGlueJobs()