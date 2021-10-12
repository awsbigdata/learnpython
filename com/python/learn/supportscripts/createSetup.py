import time
import logging
import sys, paramiko
import boto3
import json
import os

SUPPORT_TEST = 'supportTest'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# hostname = "ec2-18-207-254-90.compute-1.amazonaws.com"

home = '/Users/srramas'

ssh_key = '{}/.ssh/id_rsa.pub'.format(home)
priv_ssh_key = '{}/.ssh/id_rsa'.format(home)
# print(priv_ssh_key)

arr = [
    'cd /home/glue;source .bashrc;aws s3 cp s3://srramasdesktop/jupyterinstall/requirements.txt .;pip install -r requirements.txt --user'
    ,
    'cd /home/glue;source .bashrc;jupyter notebook --generate-config;jupyter toree install --spark_home=/usr/lib/spark --interpreters=Scala --user']


class Setup:
    def create_Endpoint(self):
        glue = boto3.client('glue')
        with open(ssh_key, 'r') as myfile:
            data = myfile.read()
        print('Dev endpoint install started')
        response = glue.create_dev_endpoint(
            EndpointName=SUPPORT_TEST,
            RoleArn='arn:aws:iam::898623153764:role/AWSGlueServiceRoleDefault',
            PublicKey=data,
            NumberOfNodes=3,
            GlueVersion='1.0',
            Arguments={"--enable-glue-datacatalog": ""}
        )
        print(response)
        status = " "
        while (status != "READY"):
            time.sleep(100)
            res = glue.get_dev_endpoint(EndpointName=SUPPORT_TEST)
            print(res)
            status = res['DevEndpoint']['Status']

        res = glue.get_dev_endpoint(EndpointName=SUPPORT_TEST)

        self.installJupyter(res['DevEndpoint']['PublicAddress'])
        self.addconfig(res['DevEndpoint']['PublicAddress'])
        self.startJupyter(res['DevEndpoint']['PublicAddress'])
        self.readoutput(res['DevEndpoint']['PublicAddress'])
        self.startsshtunnel(res['DevEndpoint']['PublicAddress'])
        print('Dev endpoint install completed')


    def getemrDNS(self):
        emr = boto3.client('emr')
        response = emr.list_clusters(
            ClusterStates=[
                'STARTING', 'RUNNING', 'WAITING'
            ]
        )
        if(len(response["Clusters"]) >0):
            cluster_response = emr.describe_cluster(ClusterId=response["Clusters"][0]['Id'])
            return cluster_response['Cluster']['MasterPublicDnsName']
        else :
            return None




    def create_emr(self):
        emr = boto3.client('emr')
        ex_cid=self.getemrDNS()
        if(ex_cid== None):
            response = emr.run_job_flow(
                Name="supportemr",
                ReleaseLabel='emr-6.2.0',
                Instances={
                    'MasterInstanceType': 'm5.xlarge',
                    'SlaveInstanceType': 'm5.2xlarge',
                    'InstanceCount': 3,
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False,
                    'Ec2KeyName': 'awssupporteast',
                    'Ec2SubnetId': 'subnet-1003d74d'
                },
                Applications=[
                    {'Name': 'Hadoop'},
                    {'Name': 'Spark'},
                    #  {'Name': 'Hbase'},
                    {'Name': 'Hive'},
                    {'Name': 'Hue'},
                    {'Name': 'Oozie'},
                   # {'Name': 'JupyterHub'},
                    {'Name': 'Presto'},
                     {'Name': 'Zeppelin'}
                ],
                VisibleToAllUsers=True,
                JobFlowRole='EMR_EC2_DefaultRole',
                ServiceRole='EMR_DefaultRole',
                ManagedScalingPolicy={
                    'ComputeLimits': {
                        'UnitType':  'Instances' ,
                        'MinimumCapacityUnits': 4,
                        'MaximumCapacityUnits': 20,
                        'MaximumOnDemandCapacityUnits': 6,
                        'MaximumCoreCapacityUnits': 5
                    }
                },
                AutoScalingRole='EMR_AutoScaling_DefaultRole',
                LogUri='s3://aws-logs-898623153764-us-east-1/elasticmapreduce/',
                Configurations=[{"Classification": "jupyter-s3-conf",
                                 "Properties": {"s3.persistence.bucket": "srramasdesktop",
                                                "s3.persistence.enabled": "true"}}
                    , {"Classification": "spark-env", "Configurations": [{"Classification": "export", "Properties": {
                        "PYSPARK3_PYTHON": "/usr/bin/python3", "PYSPARK_PYTHON": "/usr/bin/python3"}}]},
                                {"Classification": "emrfs-site",
                                 "Properties": {
                                     "aws.emrlog.enabled":"true",
                                     "fs.s3.customAWSCredentialsProvider": "com.awsamazon.external.MyAWSCredentialsProvider"}}
                                ]
                ,
                EbsRootVolumeSize=50,
                BootstrapActions=[{"ScriptBootstrapAction": {"Path": "s3://depedentjars/emrfs/configure_emrfs_lib.sh"},
                                   "Name": "EMRFS action"},
                                  {"ScriptBootstrapAction": {"Path": "s3://athenaiad/emrbr/pythonBA.sh"},
                                   "Name": "Python3library"}]

            )
            logger.info(response)
            clusterid=response['JobFlowId']
            sp.add_step(clusterid)
            time.sleep(3)
            response = emr.describe_cluster(ClusterId=clusterid)
            while(response['Cluster']['Status']['State'] in ['STARTING','BOOTSTRAPPING']):
                time.sleep(30)
                logger.info("waiting for cluster ready ...")
                response = emr.describe_cluster(ClusterId=clusterid)
            print(response['Cluster']['MasterPublicDnsName'])
            logger.info("Cluster is ready {} ".format(clusterid))
            return response['Cluster']['MasterPublicDnsName']
        else:
            print(ex_cid)
            return ex_cid

    def add_step(self, jobid):
        # First create your hive command line arguments
        emr = boto3.client('emr')
        job_flow_id = jobid
        # Split the hive args to a list
        response = emr.add_job_flow_steps(
            JobFlowId=job_flow_id,
            Steps=[
                {
                    'Name': 'Setup Hadoop Debugging',
                    'ActionOnFailure': 'TERMINATE_JOB_FLOW',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['state-pusher-script']
                    }
                },
                {
                    'Name': 'Jupyter_Lab_Install',
                    'ActionOnFailure': 'TERMINATE_JOB_FLOW',
                    'HadoopJarStep': {'Jar': 's3://depedentjars/emrfs/scriptrunner-0.0.1-SNAPSHOT.jar',
                                      'Args': ["s3://depedentjars/emrfs/emr_jupyter_step.sh"]
                                      }
                }]
        )
        logger.info(response)

    def installJupyter(self, hostname):
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            k = paramiko.RSAKey.from_private_key_file(priv_ssh_key)
            client.connect(hostname, port=22, username='glue', pkey=k)
            bashrc = """
            export PATH=/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/aws/bin:/home/glue/.local/bin:/home/glue/bin
            """
            sftp_client = client.open_sftp()
            file_handle = sftp_client.file('/home/glue/.bashrc', mode='a', bufsize=1)
            file_handle.write(bashrc)
            file_handle.flush()
            sftp_client.close()
            for pipcom in arr:
                print(pipcom)
                stdin, stdout, stderr = client.exec_command(pipcom)
                print(stdout.readlines(),)
        finally:
            client.close()

    def addconfig(self, hostname):
        time.sleep(5)
        bashrc = """
             ####create a .bashrc file #####

                # User specific aliases and functions
                export MASTER=yarn-client
                export SPARK_HOME=/usr/lib/spark
                export HADOOP_CONF_DIR=/etc/hadoop/conf
                export CLASSPATH=":/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/glue/etl/jars/*:/usr/share/aws/glue/etl/conf:/usr/share/aws/redshift/jdbc/RedshiftJDBC.jar"

                export SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS --executor-memory 5G --driver-memory 5G"
                export PYTHONPATH="/usr/lib/spark/python:/usr/lib/spark/python/lib/PySpark.zip:/usr/lib/spark/python/lib/py4j-src.zip:/usr/share/aws/glue/etl/python/PyGlue.zip:$PYTHONPATH"


                ####### end of the file 
            """
        jupyterconfig = """

                    ####Config

from s3contents import S3ContentsManager

c = get_config()

# Tell Jupyter to use S3ContentsManager for all storage.
c.NotebookApp.contents_manager_class = S3ContentsManager
c.S3ContentsManager.access_key_id = None
c.S3ContentsManager.secret_access_key =None
c.S3ContentsManager.bucket = "srramasdesktop"
c.S3ContentsManager.prefix = "gluejupyter"

####################

                    """
        ##GlueDev
        jupyer_passowrd = """
        {
  "NotebookApp": {
    "password": "sha1:eddc1cbfac1e:34b59c2d4d12f78e31874e40e2d0eb85520cad15"
  }
}
        """
        start_commd = """
                #!/bin/bash

                source /home/glue/.bashrc
                jupyter lab --no-browser --port=8888 --NotebookApp.allow_password_change=False >jupter.log 2>&1 

                """
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            k = paramiko.RSAKey.from_private_key_file(priv_ssh_key)
            client.connect(hostname, port=22, username='glue', pkey=k)
            sftp_client = client.open_sftp()
            file_handle = sftp_client.file('/home/glue/.bashrc', mode='a', bufsize=1)
            file_handle.write(bashrc)
            file_handle.flush()
            file_handle.close()
            file_handle = sftp_client.file('/home/glue/.jupyter/jupyter_notebook_config.py', mode='a', bufsize=1)
            file_handle.write(jupyterconfig.strip())
            file_handle.flush()
            file_handle.close()
            file_handle = sftp_client.file('/home/glue/.jupyter/jupyter_notebook_config.json', mode='a', bufsize=1)
            file_handle.write(jupyer_passowrd.strip())
            file_handle.flush()
            file_handle.close()
            file_handle = sftp_client.file('/home/glue/jupterstart.sh', mode='a', bufsize=1)
            file_handle.write(start_commd)
            file_handle.flush()
            file_handle.close()
        finally:
            client.close()
            sftp_client.close()

    def startJupyter(self, hostname):
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            k = paramiko.RSAKey.from_private_key_file(priv_ssh_key)
            client.connect(hostname, port=22, username='glue', pkey=k)
            startcom = ['chmod a+x /home/glue/jupterstart.sh', 'screen -d -m /home/glue/jupterstart.sh']
            for commnd in startcom:
                stdin, stdout, stderr = client.exec_command(command=commnd)
                print(stdout.readlines(),)
            # print(stdin.readlines(),)
            #  print(stderr.readlines(),)
        finally:
            client.close()

    def readoutput(self, hostname):
        time.sleep(5)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        k = paramiko.RSAKey.from_private_key_file(priv_ssh_key)
        client.connect(hostname, port=22, username='glue', pkey=k)
        sftp_client = client.open_sftp()
        remote_file = sftp_client.open('/home/glue/jupter.log')
        try:
            for line in remote_file:
                if 'localhost' in line:
                    print(line)
        finally:
            remote_file.close()
            sftp_client.close()

    def startsshtunnel(self, hostname):
        command = 'ssh -o StrictHostKeyChecking=no -i {1} -N -f -L :8888:localhost:8888 glue@{0}'.format(hostname,
                                                                                                         priv_ssh_key)
        print(command)
        os.system("netstat -nplt | grep 8888 | grep '0.0' | awk -F' ' '{print $7}' | cut -d'/' -f1 | xargs kill -9")
        os.system(command)
        print('tunnel is completed')

    def isChange(self, value):
        client = boto3.client('ec2')
        Filters = [
            {
                'Name': 'tag-key',
                'Values': [
                    'jumberhost'
                ]
            }, {
                'Name': 'instance-state-name',
                'Values': value
            }
        ]
        response = client.describe_instances(Filters=Filters)
        if (len(response['Reservations']) > 0):
            return True
        return False

    def getInstanceId(self, value):
        Filters = [
            {
                'Name': 'tag-key',
                'Values': [
                    'jumberhost'
                ]
            }, {
                'Name': 'instance-state-name',
                'Values': value
            }
        ]

        client = boto3.client('ec2')
        # instanceid=instances['Instances'][0]['InstanceId'];
        status = client.describe_instances(Filters=Filters)

        return status['Reservations'][0]['Instances'][0]['InstanceId']

    def create_ec2(self):
        Filters = [
            {
                'Name': 'tag-key',
                'Values': [
                    'jumberhost'
                ]
            }, {
                'Name': 'instance-state-name',
                'Values': [
                    'pending', 'running'
                ]
            }
        ]
        client = boto3.client('ec2')
        # create a new EC2 instance
        if (self.isChange(['stopped'])):
            #print('stopped')
            instanceId = self.getInstanceId(['stopped'])
            client.start_instances(InstanceIds=[instanceId])
        elif (not self.isChange([
            'pending', 'running',
        ])):
            #print("creating new")
            ssm = boto3.client('ssm', region_name='us-east-1')
            parameter = ssm.get_parameter(Name='/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2',
                                          WithDecryption=True)
            instances = client.run_instances(
                BlockDeviceMappings=[
                    {
                    'DeviceName':'/dev/xvda',
                        'Ebs': {
                            'DeleteOnTermination': True,
                            'VolumeSize': 100,
                            'VolumeType': 'standard',
                            'Encrypted': False
                        }
                    }],
                ImageId= parameter['Parameter']['Value'],
                MinCount=1,
                MaxCount=1,
                InstanceType='t3a.2xlarge',
                KeyName='awssupporteast',
                SecurityGroupIds=['sg-dbe372a6'],
                SubnetId='subnet-e65dabcb',
                IamInstanceProfile={"Arn":"arn:aws:iam::898623153764:instance-profile/IAMlab"},
                
                TagSpecifications=[{"ResourceType": "instance", "Tags": [{"Key": "jumberhost", "Value": "support"},{"Key": "Name", "Value": "jumperhost"}]}]
            )
        # instanceid=instances['Instances'][0]['InstanceId'];
        status = client.describe_instances(Filters=Filters)

        logger.info(status)
        while (len(status['Reservations']) < 1 or status['Reservations'][0]['Instances'][0]['State']['Name'] == 'pending'):
            time.sleep(20)
            status = client.describe_instances(Filters=Filters)
        time.sleep(3)
        print(status['Reservations'][0]['Instances'][0]['PublicDnsName'])
        # print("ssh -i /Users/srramas/personal/awssupporteast.pem ec2-user@{}".format(status['Reservations'][0]['Instances'][0]['PublicDnsName']))

    def callermethod(self, argument):
        """Dispatch method"""
        method_name = 'create_' + str(argument)
        # Get the method from 'self'. Default to a lambda.
        method = getattr(self, method_name, lambda: "Invalid month")
        # Call the method as we return it
        return method()


sp = Setup()

#print(sys.argv[1])
sp.callermethod(sys.argv[1])
# response=sp.load_cluster()
# sp.createglueEndpoint()
# sp.startInstance();
# ssh -o StrictHostKeyChecking=no -i /home/local/ANT/srramas/ssh/id_rsa -N -f -L :8888:localhost:8888 glue@ec2-54-226-131-25.compute-1.amazonaws.com

# dict ={"string",StringType(),}
