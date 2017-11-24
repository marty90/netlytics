#!/usr/bin/python

import json
import pandas as pd
import sys
from StringIO import StringIO
import pyasn
from collections import Counter, defaultdict
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

                # Parse simple fields
                clients     = Counter ((SRC,))
                queries     = Counter ((DQ,))
                resp_codes  = Counter ((DRES,))

                # Parse Returned Server IPs
                servers = Counter()
                cnames = 0
                records=str(DANS).split('|-><-|')
                for record in records:
                    if is_valid_ipv4(record):
                        servers[record]+=1
                    else:
                        cnames+=1 
                         
                # Get ASNs
                asns = Counter()
                servers_per_asn=defaultdict(Counter)
                for ip in servers:
                    try:
                        this_asn = str(asndb.lookup(ip)[0])
                        if this_asn == "None":
                            this_asn = ".".join(ip.split(".")[0:2]  ) + ".0.0"
                        if ip.startswith("127.0."):
                            this_asn=ip
                    except Exception as e:
                        this_asn=ip      
                    asns[this_asn]+=1
                    servers_per_asn[this_asn][ip] += 1

                # Get TTLs 
                ttls =  [int(e) for e in str(DANTTLS).split(",") ]

                # 1 Tuple per ASN
                for asn in asns:

                    key = ( DST, asn)
                    value = (1,queries, Counter ((cnames,)), Counter (ttls), \
                             Counter((  len(servers),  )), servers_per_asn[asn]  )

                    tup = (key,value)            

                    yield tup


# Reduce is just merging the two sets
def reduce_tuples(tup1,tup2):
    n1, queries1,clen1, ttl1, nip1, servers1 =tup1
    n2, queries2,clen2, ttl2, nip2, servers2 =tup2

    return (       n1+n2, \
                   queries1+queries2, \
                   clen1+clen2,   \
                   ttl1+ttl2,   \
                   nip1+nip2, \
                   servers1 + servers2 )
                   
# In the end, just print the Counter in a Pandas friendly format
def final_map(tup):
    (( res, asn), (n,queries,clen, ttl, nip, servers)) = tup

    n_str=str(n)
    queries_str= '"' + json.dumps(queries).replace('"','""').replace(",",";")+ '"'
    clen_str= '"' + json.dumps(clen).replace('"','""').replace(",",";")+ '"'
    ttl_str= '"' + json.dumps(ttl).replace('"','""').replace(",",";")+ '"'
    nip_str= '"' + json.dumps(nip).replace('"','""').replace(",",";")+ '"'
    servers_str = '"' + json.dumps(servers).replace('"','""').replace(",",";")+ '"'

    return ",".join([res,asn,n_str,queries_str,clen_str,ttl_str,nip_str, servers_str])

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









