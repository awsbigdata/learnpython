import boto3
from urlparse import urlparse
import csv
import datetime
today = datetime.date.today()
delteTime = today - datetime.timedelta(days = 3)

s3_input="s3://athenaiad/stage/Unsaved/2018/02/07/38"
s3_output="output.csv"
output_writer =csv.writer(open(s3_output,"w"), quoting=csv.QUOTE_ALL)

def get_s3_keys(path):
    '''
    list all the files/prefix from s3 path and write to csv file
    :param path:
    :return:
    '''
    parser_out = urlparse(path, allow_fragments=False)
    s3 = boto3.client('s3')
    print(path)
    """Get a list of keys in an S3 bucket."""
    resp = s3.list_objects_v2(Bucket=parser_out.netloc,Prefix=parser_out.path.lstrip('/'))

    for obj in resp['Contents']:
        print(obj['LastModified'])
        output_writer.writerow(["https://{}.s3.amazonaws.com/{}".format(parser_out.netloc,obj['Key'])])
        print("https://{}.s3.amazonaws.com/{}".format(parser_out.netloc,obj['Key']))



print("done")

def get_s3_paths(path):
    '''
    list all the files/prefix from s3 path and write to csv file
    :param path:
    :return:
    '''
    parser_out = urlparse(path, allow_fragments=False)
    s3 = boto3.client('s3')
    """Get a list of keys in an S3 bucket."""
    resp = s3.list_objects_v2(Bucket=parser_out.netloc,Prefix=parser_out.path.lstrip('/'))
    paths=[]
    for obj in resp['Contents']:
        paths.append("s3://{}/{}".format(parser_out.netloc,obj['Key']))

    return paths



print("start")
print(get_s3_paths(path=s3_input))
## s3.upload_file(Key,bucketName,outPutname)