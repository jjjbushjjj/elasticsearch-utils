#!/usr/bin/python

from elasticsearch import Elasticsearch, RequestsHttpConnection
from datetime import datetime, timedelta
import argparse
import certifi

parser = argparse.ArgumentParser(
        description='Process number of days to keep indices  will delete all older than <days> from Elasticsearch. Operates on logstash-* indices')
parser.add_argument('--days', metavar='D', type=int, help='number of days to keep indices', required = '1')
parser.add_argument('--server', metavar='S', help='elasticsearch server name', required = '1', action = "store")
parser.add_argument('--port', metavar='P', help='elasticsearch server port', required = '1', action = "store")
parser.add_argument('--proto', metavar='E', help='https or http', action = "store", choices = [ 'http', 'https' ], default = 'http' )
parser.add_argument('--user', metavar='u', help='Username if you are using auth', action = "store")
parser.add_argument('--password', metavar='p', help='Password if you are using auth', action = "store")
parser.add_argument('--dry', metavar='t', help='Dry run shows what will be deleted', action = "store", default = 0 , type = int )
parser.add_argument('--index', metavar='i', 
        help='Index prefix to delete (put logstash here will delete all logstash* indicies)', action = "store", required = '1' )

args = parser.parse_args()

# We keep all indices created pass this date the older will be deleted!
date_keep = datetime.today() - timedelta(days = args.days)
#print date_keep.strftime('%Y.%m.%d')

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

out = es.indices.get( args.index + '*' )

for key in out.keys():
    date = datetime.strptime( key.split("-")[-1], "%Y.%m.%d")
    if date < date_keep:
        print "%s Will be deleted!" % key
        if args.dry == 0:
            out = es.indices.delete( key )
exit(0)
