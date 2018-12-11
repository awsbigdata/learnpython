import boto3

dbname="hive_glue"
table_prefix=""

glue = boto3.client('glue', region_name='us-east-1')
tblresults = glue.get_tables(DatabaseName=dbname)

print(tblresults)
i=0;
tables=[]
print("started")

print("completed")
