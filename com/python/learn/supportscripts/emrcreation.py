import boto3

client = boto3.client('emr')

params={'Name': 'data-platform-emr-instance_fleet_test_CI',
        'LogUri': 's3://athenaiad/logs/', 'ReleaseLabel': 'emr-5.30.1', 'Instances': {'InstanceFleets': [{'InstanceFleetType': 'MASTER', 'InstanceTypeConfigs': [{'InstanceType': 'm5d.xlarge'}], 'LaunchSpecifications': {'OnDemandSpecification': {'AllocationStrategy': 'lowest-price'}}, 'Name': 'data-platform-emr-instance_fleet_test_CI-master', 'TargetOnDemandCapacity': 1}, {'InstanceFleetType': 'CORE', 'InstanceTypeConfigs': [{'InstanceType': 'm5d.xlarge'}], 'LaunchSpecifications': {'OnDemandSpecification': {'AllocationStrategy': 'lowest-price'}}, 'Name': 'data-platform-emr-instance_fleet_test_CI-core', 'TargetOnDemandCapacity': 1}, {'InstanceFleetType': 'TASK', 'InstanceTypeConfigs': [{'InstanceType': 'm5a.xlarge'}], 'LaunchSpecifications': {'OnDemandSpecification': {'AllocationStrategy': 'lowest-price'}}, 'Name': 'data-platform-emr-instance_fleet_test_CI-task', 'TargetOnDemandCapacity': 1}], 'Ec2KeyName': 'data-platform-emr-cinp-key', 'KeepJobFlowAliveWhenNoSteps': True, 'TerminationProtected': False, 'Ec2SubnetIds': ['subnet-bf55ca90', 'subnet-d9e97a84', 'subnet-e5a782ae'], 'EmrManagedMasterSecurityGroup': 'sg-003a66f8e0f82967a', 'EmrManagedSlaveSecurityGroup': 'sg-05225eed2aec2b758', 'ServiceAccessSecurityGroup': '', 'AdditionalMasterSecurityGroups': [''], 'AdditionalSlaveSecurityGroups': ['']}, 'BootstrapActions': [{'Name': 'custom_bootstrap', 'ScriptBootstrapAction': {'Args': [], 'Path': 's3://data-platform-emr-596908489536-us-east-1/scripts/emr_bootstrap.sh'}}], 'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}], 'Configurations': [{'Classification': 'hive-site', 'Properties': {'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}}, {'Classification': 'spark-defaults', 'Properties': {'spark.blacklist.decommissioning.enabled ': 'false', 'spark.driver.extraJavaOptions': "-XX:OnOutOfMemoryError='kill -9 %p' -XX:+UseG1GC -XX:+ParallelRefProcEnabled -XX:ParallelGCThreads=5 -XX:-ResizePLAB -XX:PretenureSizeThreshold=1048576", 'spark.executor.extraJavaOptions': "-XX:OnOutOfMemoryError='kill -9 %p' -XX:+UseG1GC -XX:+ParallelRefProcEnabled -XX:ParallelGCThreads=5 -XX:-ResizePLAB -XX:PretenureSizeThreshold=1048576 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps"}}, {'Classification': 'spark-hive-site', 'Properties': {'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}}, {'Classification': 'yarn-site', 'Properties': {'yarn.nodemanager.pmem-check-enabled': 'false', 'yarn.nodemanager.vmem-check-enabled': 'false', 'yarn.resourcemanager.connect.retry-interval.ms': '500'}}], 'VisibleToAllUsers': True, 'JobFlowRole': 'arn:aws:iam::596908489536:instance-profile/acct-managed/data-platform-emr-ec2-role', 'ServiceRole': 'arn:aws:iam::596908489536:role/acct-managed/data-platform-emr-role', 'Tags': [{'Key': 'application', 'Value': 'data-platform-emr'}, {'Key': 'application_name', 'Value': 'instance_fleet_test_CI'}, {'Key': 'project_name', 'Value': 'data-platform-executors'}, {'Key': 'team', 'Value': 'EDPSirius@coxautoinc.com'}, {'Key': 'environment', 'Value': 'cinp'}, {'Key': 'Ephemeral', 'Value': 'True'}, {'Key': 'Name', 'Value': 'data-platform-emr-instance_fleet_test_CI'}, {'Key': 'wc_component_id', 'Value': 'CI0586012'}, {'Key': 'wc_component_name', 'Value': 'EXECUTORS'}, {'Key': 'wc_workload_id', 'Value': 'CI0585877'}, {'Key': 'wc_workload_name', 'Value': 'Data Platform Data Services'}, {'Key': 'managed_by', 'Value': 'terraform'}, {'Key': 'short_name', 'Value': 'instance_fleet_test_CI'}], 'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION', 'StepConcurrencyLevel': 256}

response = client.run_job_flow(**params)

print(response)
