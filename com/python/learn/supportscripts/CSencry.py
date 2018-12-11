import aws_encryption_sdk
import boto3
import botocore.session
import base64

def build_multiregion_kms_master_key_provider():
    regions = ['us-east-1']
    alias = 'alias/35e34cd9-5980-4352-fdssd-38fcabeef24d'
    arn_template = 'arn:aws:kms:{region}:{account_id}:{alias}'

    # Create AWS KMS master key provider
    kms_master_key_provider = aws_encryption_sdk.KMSMasterKeyProvider()
    existing_botocore_session = botocore.session.Session()
    kms_key_provider = aws_encryption_sdk.KMSMasterKeyProvider(key_ids=[
        'arn:aws:kms:us-east-1:898623153764:key/35e34cd9-5980-4352-a900-38fcabeef24d'
    ])
    return kms_key_provider

def encrypt_file(file_name):
    # Get all the master keys needed
    key_provider = build_multiregion_kms_master_key_provider()

    # Encrypt the provided data
    ciphertext, header = aws_encryption_sdk.encrypt(
        source=open(file_name, 'rb'),
        key_provider=key_provider
    )
    print(type(header).__name__)
    print(header)
    print(header.encryption_context)


    return ciphertext


encrypt_file('/home/local/ANT/srramas/Downloads/test.json')



#s3 = boto3.client('s3')
#s3.put_object(Bucket='athenaiad', Body=open('/home/local/ANT/srramas/Downloads/test.json.enc', 'r'),
#Key='apytest/test.json', Metadata={'x-amz-meta-x-amz-key-v2': base64.b64encode(data_key_ciphered),'x-amz-meta-x-amz-wrap-alg':'kms','Content-Type':'application/octet-stream','x-amz-meta-x-amz-cek-alg':'AES/CBC/PKCS5Padding','x-amz-meta-x-amz-matdesc':'{"kms_cmk_id":"35e34cd9-5980-4352-fdssd-38fcabeef24d"}'})

