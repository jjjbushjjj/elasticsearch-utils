#!/usr/bin/python

from elasticsearch import Elasticsearch, RequestsHttpConnection
import argparse
import certifi

parser = argparse.ArgumentParser(
        description='Searches for indecies without any documents')
parser.add_argument('--server', metavar='S', help='elasticsearch server name', required = '1', action = "store")
parser.add_argument('--port', metavar='P', help='elasticsearch server port', required = '1', action = "store")
parser.add_argument('--proto', metavar='E', help='https or http', action = "store", choices = [ 'http', 'https' ], default = 'http' )
parser.add_argument('--user', metavar='u', help='Username if you are using auth', action = "store")
parser.add_argument('--password', metavar='p', help='Password if you are using auth', action = "store")
parser.add_argument('--delete', metavar='d', help='Delete empty indicies', choices = ['yes', 'no' ], default = 'no', action = "store")


args = parser.parse_args()

if args.proto == 'https':
    use_ssl = True
    # key and cert for client if using ssl
    client_certs = ('el-abushuev.cert.pem', 'el-abushuev.key.pem')
    ca_bundle = "ca.cert.pem"
else:
    use_ssl = False

es = Elasticsearch( connection_class=RequestsHttpConnection,
                    hosts = [ args.server ],
                    https_auth = ( args.user, args.password ),
                    port = int(args.port),
                    use_ssl = use_ssl,
                    verify_certs=True,
                    ca_certs = ca_bundle,
                    client_cert = client_certs,
                )


out = es.cat.indices( index = '*', h = ('index','docs.count') )
out = out.strip()
index_to_delete = []

res = dict( item.split() for item in out.split("\n") )

for index,size in res.items():
    if size == '0':
        index_to_delete.append( index )
        print index

# This will delete all empty indicies
if index_to_delete and args.delete == 'yes':
    print "I will DELETE those indicies!"
    out = es.indices.delete( index_to_delete )


exit(0)
