import boto3
import logging
from .ResourceTemplate import *
from botocore.exceptions import ClientError

class EMRClusterResource(Resource):

    TEMPLATE_URL = ('https://<region>.console.aws.amazon.com/'
                    + 'elasticmapreduce/home?region=<region>#'
                    + 'cluster-details:<id>'
    )

    def _set_service(self, service='emr'):
        return service

    def get(self, tags=[]):
        response = self.client.list_clusters(
                ClusterStates=['RUNNING','WAITING']
        )

        dbs = []

        for r in response['Clusters']:
            try:

                cluster = self.client.describe_cluster(ClusterId=r['Id'])
                dbs.append({
                    'id': r['Id'],
                    'name': r['Name'],
                    'created': cluster['Cluster']['Status']['Timeline']['CreationDateTime'],
                    'tags': cluster['Cluster']['Tags'],
                    'terminationProtection': cluster['Cluster']['TerminationProtected']
                })
            except ClientError as e:
                logging.info('{}'.format(e))
                raise

        return dbs

    def delete(self, resource):
        try:
            if resource['terminationProtection']:
                logging.info('Disabling TP on {}'.format(
                    resource['id'])
                )
                response = self.client.set_termination_protection(
                    JobFlowIds = [resource['id']],
                    TerminationProtected = False
                )
                self.client.terminate_job_flows(
                    JobFlowIds=[resource['id']]
                )
        except ClientError as e:
            logging.warn('{}'.format(e))
            raise

        else:
            self.client.terminate_job_flows(
                JobFlowIds=[resource['id']]
            )

        return None
