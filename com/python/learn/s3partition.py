from boto3.s3.transfer import S3Transfer
import boto3
import shutil
import os, errno
filepath='/home/local/ANT/srramas/Downloads/test_out.json'
bucket_name='athena-organ-test'
folder_name='crawlertest'
filename='test.json'
inpath='/home/local/ANT/srramas/Downloads/test/'
#have all the variables populated which are required below
client = boto3.client('s3')
transfer = S3Transfer(client)

month=1
year=2060
day=1;

for i in range(1000):
    if(month %12==0):
        year+=1
    if(i%30==0):
        month+=1
    day=i%30
    path=inpath+folder_name+'/year='+str(year)+"/month="+str(month)+"/day="+str(day)+"/"+str(i)+"_"+filename
    directory = os.path.dirname(path)
    print(directory)
    if not os.path.exists(directory):
        os.makedirs(directory)
    shutil.copy(filepath,path)
   # transfer.upload_file(filepath, bucket_name, path+"/"+str(i)+"_"+filename)
    print("no of file",i)