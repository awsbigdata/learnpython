import boto3
from urlparse import urlparse
import csv

s3_input="s3://athenademo-youtube"
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
    resp = s3.list_objects_v2(Bucket=parser_out.netloc)
    for obj in resp['Contents']:
        print(obj)
        output_writer.writerow(["https://{}.s3.amazonaws.com/{}".format(parser_out.netloc,obj['Key'])])
        print("https://{}.s3.amazonaws.com/{}".format(parser_out.netloc,obj['Key']))



get_s3_keys(s3_input)
## s3.upload_file(Key,bucketName,outPutname)