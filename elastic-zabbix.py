#!/usr/bin/python

from elasticsearch import Elasticsearch, RequestsHttpConnection
import argparse
import certifi
import json

client_certs = ('el-abushuev.cert.pem', 'el-abushuev.key.pem')
ca_bundle = "ca.cert.pem"
el_server = 'el-esbaccess01'
el_port = 443

es = Elasticsearch( connection_class=RequestsHttpConnection,
                    hosts = [ el_server ],
                    port = el_port,
                    use_ssl = True,
                    verify_certs=True,
                    ca_certs = ca_bundle,
                    client_cert = client_certs,
                )

#out = es.info()
out = es.nodes.stats( metric = 'jvm' )
print json.dumps(out, indent =3 )
exit(0)
