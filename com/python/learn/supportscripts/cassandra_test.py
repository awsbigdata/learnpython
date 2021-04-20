#from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
#from cassandra.auth import PlainTextAuthProvider
ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.load_verify_locations("/Users/srramas/cassandra_ssl/sf-class2-root.crt")
ssl_context.verify_mode = CERT_REQUIRED
print("able to reach here")