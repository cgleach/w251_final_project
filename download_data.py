import boto
import boto.s3.connection
from boto.s3.key import Key
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv( verbose = True )

secret_key = os.getenv("SECRET_KEY")
access_key = os.getenv("ACCESS_KEY")
conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = 's3-api.us-geo.objectstorage.softlayer.net',
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

bucket = conn.get_bucket('final-project')

k = Key(bucket)
k.key = 'addresses_west'
k.get_contents_to_filename('hello.txt')

#files = ['addresses_midwest', 'addresses_northeast', 'addresses_south', 'addresses_west', 'tax_return_data']

#for file in files:
#    k.key = file
#    pathway = '~/data/'
#    print(pathway)
#    k.get_contents_to_filename(file)
