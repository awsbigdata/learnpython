from Crypto import Random
from Crypto.Cipher import AES
import boto3
import base64

def pad(s):
    return s + b"0" *(AES.block_size - len(s) % AES.block_size)

def encrypt(message, key):
    message = pad(message)
    print(message)
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return iv + cipher.encrypt(message)

def decrypt(ciphertext, key):
    iv = ciphertext[:AES.block_size]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plaintext = cipher.decrypt(ciphertext[AES.block_size:])
    return plaintext.rstrip(b"\0")

def encrypt_file(file_name, key):
    with open(file_name, 'rb') as fo:
        plaintext = fo.read()
    enc = encrypt(plaintext, key)
    with open(file_name + ".enc", 'wb') as fo:
        fo.write(enc)

def decrypt_file(file_name, key):
    with open(file_name, 'rb') as fo:
        ciphertext = fo.read()
    dec = decrypt(ciphertext, key)
    with open(file_name[:-4], 'wb') as fo:
        fo.write(dec)



kms = boto3.client('kms')
data_key_req = kms.generate_data_key(KeyId='35e34cd9-5980-4352-a900-38fcabeef24d', KeySpec='AES_256')
data_key = data_key_req['Plaintext']
data_key_ciphered = data_key_req['CiphertextBlob']

encrypt_file('/home/local/ANT/srramas/Downloads/test.csv', data_key)

print(base64.b64encode(data_key_ciphered))

s3 = boto3.client('s3')
s3.put_object(Bucket='athenaiad', Body=open('/home/local/ANT/srramas/Downloads/test.csv.enc', 'r'),
Key='apytest/test.csv', Metadata={'x-amz-meta-x-amz-key-v2': base64.b64encode(data_key_ciphered),'x-amz-meta-x-amz-wrap-alg':'kms','Content-Type':'application/octet-stream','x-amz-meta-x-amz-cek-alg':'AES/CBC/PKCS5Padding','x-amz-meta-x-amz-matdesc':'{"kms_cmk_id":"35e34cd9-5980-4352-fdssd-38fcabeef24d"}'})


