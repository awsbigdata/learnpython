import boto3

class RunResource:
    redshift= boto3.client('redshift')
    def createRedshift(self):
        clusterid='redshift'
        response = self.redshift.create_cluster(
            DBName='reddb',
            ClusterIdentifier=clusterid,
            NodeType='dc2.large',
            MasterUsername='reduser',
            MasterUserPassword='Redpassw0rd',
            VpcSecurityGroupIds=[
                'sg-12c9176e'
            ],
            ClusterSubnetGroupName='publicsubnet',
            ClusterParameterGroupName='customparametergroup',
            NumberOfNodes=2,
            PubliclyAccessible=True,
            Tags=[
                {
                    'Key': 'test',
                    'Value': 'test'
                },
            ],
            IamRoles=['arn:aws:iam::898623153764:role/redshiftspectrumrole']
        )

        response = self.redshift.describe_clusters(ClusterIdentifier=clusterid)
        for cluster in response['Clusters']:
            print(cluster['ClusterStatus'])

        print(response)

run=RunResource()
run.createRedshift()

