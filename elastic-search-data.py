#!/usr/bin/python

from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
import argparse
import certifi
import json

parser = argparse.ArgumentParser(
        description='Searches for indecies without any documents')
parser.add_argument('--server', metavar='S', help='elasticsearch server name', required = '1', action = "store")
parser.add_argument('--port', metavar='P', help='elasticsearch server port', required = '1', action = "store")
parser.add_argument('--proto', metavar='E', help='https or http', action = "store", choices = [ 'http', 'https' ], default = 'http' )
parser.add_argument('--user', metavar='u', help='Username if you are using auth', action = "store")
parser.add_argument('--password', metavar='p', help='Password if you are using auth', action = "store")


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


# Ok we hopefully get the connection

indx = 'membrane-2015.07.08'
query={"query" : {"match_all" : {}}}
time_range = '1d'

#out = helpers.scan(es, query=query, index=indx, scroll=time_range, preserve_order=True)

scanResp = es.search(index=indx, body=query, search_type="scan", scroll="1d")  
scrollId = scanResp['_scroll_id']

response = es.scroll(scroll_id=scrollId, scroll= "1d")
print json.dumps(response, indent=3)

#matches = es.search( indx, q = query, fields = ('message','@timestamp'), size = 10 )
#hits = matches['hits']['hits']

#print json.dumps(matches, indent=3)

#if not hits:
#    print "No matches found"
#    exit(1)

#for hit in hits:
#    print hit['fields']['@timestamp'][0], 
#    print hit['fields']['message'][0]


#print repr(out)

exit(0)
