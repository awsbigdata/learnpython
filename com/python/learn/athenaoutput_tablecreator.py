import boto3
import json
import re


##query id which you need to create the table

queryid="8fe9df99-96a8-438e-acdf-85d1b1de1e68"

###output table prefix

prefix="atheout_"

### create a new folder and output file will be copied to below directory
s3output="s3://athenaiad/athenaoutput/"

##table create database
output_database="hive_glue"

client = boto3.client('athena',region_name='us-east-1')

print(" ######### Started ########")

res_out = client.get_query_execution(
    QueryExecutionId=queryid
)

print("\n New table will be create for query string {0} \n".format(res_out['QueryExecution']['Query']))

athena_output=res_out['QueryExecution']['ResultConfiguration']['OutputLocation']

response = client.get_query_results(
    QueryExecutionId=queryid, MaxResults=1)

#print(response)


table_create="CREATE EXTERNAL TABLE  {0}{1} ( ".format(prefix,queryid).replace("-","")

for col in response['ResultSet']['ResultSetMetadata']['ColumnInfo']:
    col_type=json.dumps(col['Type']).replace("\"","")
    if(col_type=="bigint"):
        table_create=table_create+json.dumps(col['Name']).replace("\"","")+" "+col_type+","
    else:
        table_create = table_create + json.dumps(col['Name']).replace("\"", "") + " string ,"

table_create = table_create.rstrip(',')
table_create=table_create+""" ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '\\"' 
   )
STORED AS TEXTFILE
LOCATION '{0}{1}' TBLPROPERTIES ('skip.header.line.count'='1')""" .format(s3output,queryid)

print(table_create)


s3 = boto3.resource('s3')
match = re.match(r's3:\/\/(.+?)\/(.+)', athena_output)
source= { 'Bucket' : str(match.group(1)), 'Key': str(match.group(2))}

dest=re.match(r's3:\/\/(.+?)\/(.+)', s3output)

s3.meta.client.copy(source,str(dest.group(1)),str(dest.group(2))+"{0}/{0}.csv".format(queryid))

response = client.start_query_execution(
    QueryString=table_create,
    QueryExecutionContext={
        'Database': output_database
    },
    ResultConfiguration={
        'OutputLocation': s3output+"temp/",

    }
)
print(response)

print("######## Completed ########")
#s3.meta.client.download_file('testeastreg', 'output/'+queryid+'.csv', '/tmp/'+queryid+'.csv')