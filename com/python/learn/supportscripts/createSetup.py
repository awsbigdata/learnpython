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

hostname = "ec2-18-207-254-90.compute-1.amazonaws.com"

ssh_key = '/home/shiva/ssh/id_rsa.pub'
priv_ssh_key = '/home/shiva/ssh/id_rsa'
arr = [
    'curl -o- https://raw.githubusercontent.com/creationix/nvm/v0.32.0/install.sh | bash;. ~/.nvm/nvm.sh;nvm install 8.0.0',
    'cd /home/glue;source .bashrc;aws s3 cp s3://srramasdesktop/jupyterinstall/requirements.txt .;pip install -r requirements.txt --user'
    ,
    'cd /home/glue;source .bashrc;jupyter notebook --generate-config;jupyter toree install --spark_home=/usr/lib/spark --interpreters=Scala --user;jupyter labextension install @jupyterlab/git;jupyter serverextension enable --py jupyterlab_git']


class Setup:
    def createglueEndpoint(self):
        glue = boto3.client('glue')
        with open(ssh_key, 'r') as myfile:
            data = myfile.read()
        print('Dev endpoint install started')
        response = glue.create_dev_endpoint(
            EndpointName=SUPPORT_TEST,
            RoleArn='arn:aws:iam::898623153764:role/AWSGlueServiceRoleDefault',
            PublicKey=data,
            NumberOfNodes=3,
            Arguments={
                "--enable-glue-datacatalog": ""
            }
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

    def load_cluster(self):
        glue = boto3.client('emr')
        response = glue.run_job_flow(
            Name="test",
            ReleaseLabel='emr-5.20.0',
            Instances={
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2KeyName': 'awssupporteast',
                'InstanceFleets': [{"InstanceFleetType": "MASTER", "TargetOnDemandCapacity": 1, "TargetSpotCapacity": 0,
                                    "InstanceTypeConfigs": [{"WeightedCapacity": 1, "EbsConfiguration": {
                                        "EbsBlockDeviceConfigs": [
                                            {"VolumeSpecification": {"SizeInGB": 100, "VolumeType": "gp2"},
                                             "VolumesPerInstance": 1}], "EbsOptimized": True},
                                                             "BidPriceAsPercentageOfOnDemandPrice": 100,
                                                             "InstanceType": "m4.xlarge"}], "Name": "Master - 1"},
                                   {"InstanceFleetType": "CORE", "TargetOnDemandCapacity": 1, "TargetSpotCapacity": 1,
                                    "LaunchSpecifications": {"SpotSpecification": {"TimeoutDurationMinutes": 10,
                                                                                   "TimeoutAction": "SWITCH_TO_ON_DEMAND",
                                                                                   "BlockDurationMinutes": 60}},
                                    "InstanceTypeConfigs": [
                                        {"WeightedCapacity": 1, "BidPriceAsPercentageOfOnDemandPrice": 100,
                                         "InstanceType": "r5d.2xlarge"},
                                        {"WeightedCapacity": 1, "BidPriceAsPercentageOfOnDemandPrice": 100,
                                         "InstanceType": "h1.2xlarge"},
                                        {"WeightedCapacity": 1, "BidPriceAsPercentageOfOnDemandPrice": 100,
                                         "InstanceType": "i3.2xlarge"}], "Name": "Core - 2"}],
                'Ec2SubnetIds': ["subnet-0586e44466984e292", "subnet-07dde1960a72fa516", "subnet-0e8816c7392a9b3c8",
                                 "subnet-1003d74d", "subnet-e65dabcb"]

            },
            ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            EbsRootVolumeSize=20,
            Applications=[
                {'Name': 'Hadoop'},
                {'Name': 'Spark'},
                #  {'Name': 'Hbase'},
                {'Name': 'Hive'},
                {'Name': 'Hue'},
                {'Name': 'Oozie'},
                {'Name': 'JupyterHub'},
                {'Name': 'Presto'},
                # {'Name': 'Zeppelin'}
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            LogUri='s3://aws-logs-898623153764-us-east-1/elasticmapreduce/',
            Configurations=[{"Classification": "jupyter-s3-conf",
                             "Properties": {"s3.persistence.bucket": "srramasdesktop",
                                            "s3.persistence.enabled": "true"}}
                , {"Classification": "spark-env", "Configurations": [{"Classification": "export", "Properties": {
                    "PYSPARK3_PYTHON": "/usr/bin/python3", "PYSPARK_PYTHON": "/usr/bin/python3"}}]},
                            {"Classification": "emrfs-site",
                             "Properties": {
                                 "fs.s3.customAWSCredentialsProvider": "com.awsamazon.external.MyAWSCredentialsProvider"}}]
            ,
            BootstrapActions=[{"ScriptBootstrapAction": {"Path": "s3://depedentjars/emrfs/configure_emrfs_lib.sh"},
                               "Name": "Custom action"}]

        )
        logger.info(response)
        sp.add_step(response['JobFlowId'])
        return response

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
                    'HadoopJarStep': {'Jar': 's3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',
                                      'Args': ["s3://depedentjars/emrfs/emr_jupyter_lab.sh"]
                                      }
                }]
        )
        print(response)

    def installJupyter(self, hostname):
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(hostname, port=22, username='glue', password='', key_filename=priv_ssh_key)
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
                export PYTHONPATH="/usr/lib/spark/python:/usr/lib/spark/python/lib/PySpark.zip:/usr/lib/spark/python/lib/py4j-0.10.4-src.zip:/usr/share/aws/glue/etl/python/PyGlue.zip:$PYTHONPATH"

                export PYSPARK_DRIVER_PYTHON_OPTS="notebook /usr/bin/gluepyspark"

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
            client.connect(hostname, port=22, username='glue', password='', key_filename=priv_ssh_key)
            sftp_client = client.open_sftp()
            file_handle = sftp_client.file('/home/glue/.bashrc', mode='a', bufsize=1)
            file_handle.write(bashrc)
            file_handle.flush()
            file_handle = sftp_client.file('/home/glue/.jupyter/jupyter_notebook_config.py', mode='a', bufsize=1)
            file_handle.write(jupyterconfig.strip())
            file_handle.flush()
            file_handle = sftp_client.file('/home/glue/.jupyter/jupyter_notebook_config.json', mode='a', bufsize=1)
            file_handle.write(jupyer_passowrd.strip())
            file_handle.flush()
            file_handle = sftp_client.file('/home/glue/jupterstart.sh', mode='a', bufsize=1)
            file_handle.write(start_commd)
            file_handle.flush()
        finally:
            client.close()
            sftp_client.close()

    def startJupyter(self, hostname):
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(hostname, port=22, username='glue', password='', key_filename=priv_ssh_key)
            startcom = ['chmod a+x /home/glue/jupterstart.sh', 'screen -d -m /home/glue/jupterstart.sh']
            for commnd in startcom:
                stdin, stdout, stderr = client.exec_command(command=commnd)
                print(stdout.readlines(),)
                # print(stdin.readlines(),)
                #  print(stderr.readlines(),)
        finally:
            client.close()

    def readoutput(self, hostname):
        time.sleep(10)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname, port=22, username='glue', password='', key_filename=priv_ssh_key)
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
        command = 'ssh -o StrictHostKeyChecking=no -i /home/shiva/ssh/id_rsa -N -f -L :8888:localhost:8888 glue@{0}'.format(
            hostname)
        print(command)
        os.system("netstat -nplt | grep 8888 | grep '0.0' | awk -F' ' '{print $7}' | cut -d'/' -f1 | xargs kill -9")
        os.system(command)
        print('tunnel is completed')


sp = Setup()

response = sp.load_cluster()
# sp.createglueEndpoint()
# ssh -o StrictHostKeyChecking=no -i /home/local/ANT/shiva/ssh/id_rsa -N -f -L :8888:localhost:8888 glue@ec2-54-145-188-165.compute-1.amazonaws.com
