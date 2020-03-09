from __future__ import absolute_import, print_function, unicode_literals
import boto3
from gzip import GzipFile
from io import BytesIO
s3 = boto3.client('s3')
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
        obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        bytestream = BytesIO(obj['Body'].read())
        got_text = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
        file_name = object_key.rstrip('.gz')
        encoded_string = got_text.encode("utf-8")
        s3.put_object(Bucket=bucket_name,Key=file_name,Body=encoded_string)
        response = s3.delete_object(Bucket=bucket_name, Key=object_key)
        logger.info(str(response))
    return "success"