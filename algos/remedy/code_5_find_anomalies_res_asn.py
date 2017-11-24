#!/usr/bin/python3
from __future__ import print_function
from __future__ import division
import json
import pandas as pd
from numpy import mean, absolute
import numpy
import math
from collections import defaultdict
from collections import Counter
import functools
import sys
from math import ceil, floor

MIN_RATIO=2
MIN_2LD_DOMAINS=3

def find_anomalies (in_aggregated,in_params,in_anomalies,out_anomalies_res_asn):

    # Parse input
    anomalies_res_domain = { line.split(",")[0] + " " + line.split(",")[1]  \
                  for line in open(in_anomalies,"r").read().splitlines() \
                  if "cagnaccio" not in line.split(",")[2] }

    params = json.load(open(in_params,"r"))
    MIN_2LD_DOMAINS = params["SLD_ASN"][3][1]
    MIN_RATIO=params["SLD_COUNT"][3][1]


    # Open Aggregated Log and parse it
    data = {}
    for line in open(in_aggregated,"r").read().splitlines():

        fields=line.split(",")
        resolver=fields[0]
        asn = fields[1]

        count = int(fields[2])
        
        queries = json_to_counter(fields[3])
        queries_2ld = { getGood2LD(domain) for domain in queries }
        n_queries=len( queries )
        n_queries_2ld=len( queries_2ld )
      
        cnames=  json_to_counter(fields[4])
        ttls=json_to_counter(fields[5])
        nip=json_to_counter(fields[6])
        servers=json_to_counter(fields[7])

        # Update per domain data
        domain_query_info={"count":count, "n_queries":n_queries, "n_queries_2ld":n_queries_2ld,\
                          "queries_2ld": queries_2ld, "queries" : queries,\
                          "cnames":cnames, "ttls":ttls, "nip":nip, "servers":servers}
        data[resolver + " " + asn]=domain_query_info


    anomalies = []

    for key in data:
        # Get stats
        resolver, asn = key.split()
        count = data[key]["count"]
        queries_2ld=data[key]["queries_2ld"]
        queries=data[key]["queries"]
        n_queries = data[key]["n_queries"]
        n_queries_2ld = data[key]["n_queries_2ld"]
        cnames = data[key]["cnames"]
        ttls = data[key]["ttls"]
        nip = data[key]["nip"]
        servers = data[key]["servers"]

        # Count fixed features
        anomaly_features = []
        for feature, counter in {"ttl":ttls, "cname":cnames, "nip":nip}.items():
            if len(counter) == 1:
                anomaly_features.append(feature)

        # Print resulting anomalies
        if count/n_queries_2ld > MIN_RATIO and n_queries_2ld > MIN_2LD_DOMAINS and \
           len (anomaly_features) > 1:
            anomalies.append (  (resolver, asn , count, n_queries_2ld , \
                   json.dumps(ttls).replace(" ",""), \
                   json.dumps(cnames).replace(" ",""), \
                   json.dumps(nip).replace(" ",""),\
                   ":".join(queries_2ld) ,\
                   ":".join(queries)  ,\
                   ":".join(servers),\
                   ":".join(anomaly_features)  ) )



    fo = open(out_anomalies_res_asn,"w")
    print ("res domain asn servers fixed_features", file = fo)
    for anomaly in anomalies:
        (res, asn, count, n_queries_2ld, ttl, clen, nip, queries_2ld, queries, servers, fixed_features) = anomaly
        domains = queries.split(":")
        for domain in domains:
            lookup = res + " " + domain
            if lookup in anomalies_res_domain:
                print (res, domain, asn, servers, fixed_features, file = fo)
 
    fo.close()



def json_to_counter(s):
    s_clean=s.replace('""','"').replace(";",",")[1:-1]
    return Counter( json.loads(s_clean))


bad_domains=set("co.uk co.jp co.hu co.il com.au co.ve .co.in com.ec com.pk co.th co.nz com.br com.sg com.sa \
com.do co.za com.hk com.mx com.ly com.ua com.eg com.pe com.tr co.kr com.ng com.pe com.pk co.th \
com.au com.ph com.my com.tw com.ec com.kw co.in co.id com.com com.vn com.bd com.ar \
com.co com.vn org.uk net.gr".split())

# Cut a domain after 2 levels
# e.g. www.google.it -> google.it
def get2LD(fqdn):
    if fqdn[-1] == ".":
        fqdn = fqdn[:-1]    
    names = fqdn.split(".")
    tln_array = names[-2:]
    tln = ""
    for s in tln_array:
        tln = tln + "." + s
    return tln[1:]

def getGood2LD(fqdn):
    if fqdn[-1] == ".":
        fqdn = fqdn[:-1]    
    names = fqdn.split(".")
    if ".".join(names[-2:]) in bad_domains:
        return get3LD(fqdn)
    tln_array = names[-2:]
    tln = ""
    for s in tln_array:
        tln = tln + "." + s
    return tln[1:]

# Cut a domain after 3 levels
# e.g. www.c3.google.it -> c3.google.it
def get3LD(fqdn):
    if fqdn[-1] == ".":
        fqdn = fqdn[:-1]
    names = fqdn.split(".")
    tln_array = names[-3:]
    tln = ""
    for s in tln_array:
        tln = tln + "." + s
    return tln[1:]

