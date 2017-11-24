#!/usr/bin/python

import json
import pandas as pd
import sys
from StringIO import StringIO
import pyasn
from collections import Counter
from pyspark import SparkConf, SparkContext


def emit_tuples(lines):

    # Create a pyasn to get ASNs
    asndb = pyasn.pyasn('ASN_VIEW')
    
    # Iterate over the lines
    for line in lines:

        if line.flags_resp != "-":
            DRES= line.resp_code
            DFRD = "1" if int(line.flags_resp) & 0x08 != 0 else "0"
            DFRA = "1" if int(line.flags_resp) & 0x04 != 0 else "0"          
            DANS = '|-><-|'.join(line.answers)
            DANTTLS = ','.join([ str (t) for t in line.answer_ttls] )
            DST= line.s_ip
            DQ = line.query
            SRC = line.c_ip

            # Get Only Recursive Queries
            if DRES == "NOERROR" and DFRD == "1" and DFRA == "1":

                # Create Key            
                key = DST

                # Parse simple fields
                clients     = set ((SRC,))
                queries     = set ((DQ,))

                # Parse Returned Server IPs
                servers = set()
                records=str(DANS).split('|-><-|')
                for record in records:
                    if is_valid_ipv4(record):
                        servers.add(record)

                         
                # Get ASNs
                asns = set()
                for ip in servers:
                    try:
                        this_asn = str(asndb.lookup(ip)[0])
                        if this_asn == "None":
                            this_asn = ".".join(ip.split(".")[0:2]  ) + ".0.0"
                        if ip.startswith("127.0."):
                            this_asn=ip
                    except Exception as e:
                        this_asn=ip      
                    asns.add(this_asn)

                value = (1,clients,queries,servers,asns)

                # Produce an output tuple
                tup = (key,value)            

                yield tup


# Reduce is just merging the two sets
def reduce_tuples(tup1,tup2):
    n1, clients1,queries1,servers1,asns1=tup1
    n2, clients2,queries2,servers2,asns2=tup2

    ret = (       n1+n2, \
                   clients1|clients2, \
                   queries1|queries2, \
                   servers1|servers2,   \
                   asns1|asns2  )


    return ret
                   
# In the end, just print the Counter in a Pandas friendly format
def final_map(tup):
    (res, (n,clients,queries,servers,asns)) = tup

    n_str=str(n)
    clients_str='"' + json.dumps(list(clients)).replace('"','""').replace(",",";")+ '"'
    queries_str= '"' + json.dumps(list(queries)).replace('"','""').replace(",",";")+ '"'
    servers_str= '"' + json.dumps(list(servers)).replace('"','""').replace(",",";")+ '"'
    asns_str= '"' + json.dumps(list(asns)).replace('"','""').replace(",",";")+ '"'

    return ",".join([res,n_str,clients_str,queries_str,servers_str,asns_str])

# Check if an IPv4 is valid
def is_valid_ipv4(s):
    a = s.split('.')
    if len(a) != 4:
        return False
    for x in a:
        if not x.isdigit():
            return False
        i = int(x)
        if i < 0 or i > 255:
            return False
    return True

def parse_line(line):
    fields = []
    current_field=""
    in_quote=False
    for c in line:
        if not in_quote and c == ",":
            fields.append(current_field)
            current_field=""
        elif in_quote and c == '"':
            in_quote=False
        elif not in_quote and c == '"':
            in_quote=True
        else:
            current_field+=c
    fields.append(current_field)
    return fields    


