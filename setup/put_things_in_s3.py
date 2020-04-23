import boto
import boto.s3.connection

conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = 's3-api.us-geo.objectstorage.softlayer.net',
        #is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

bucket = conn.get_bucket('final-project')

from boto.s3.key import Key
k = Key(bucket)
#k.key = 'addresses_south'
#k.set_contents_from_filename('openaddr-collected-us_south.zip')

#k.key = 'addresses_west'
#k.set_contents_from_filename('openaddr-collected-us_west.zip')

#k.key = 'addresses_midwest'
#k.set_contents_from_filename('openaddr-collected-us_midwest.zip')

k.key = 'addresses_northeast'
k.set_contents_from_filename('openaddr-collected-us_northeast.zip')
