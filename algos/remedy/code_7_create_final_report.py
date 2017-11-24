#!/usr/bin/python3

import functools
import json
import math
import numpy
import pandas as pd
import sys
from collections import Counter
from collections import defaultdict
from math import ceil, floor
from numpy import mean, absolute


def create_report (in_aggregated,in_anomalies,out_final_report ):


    # Open Aggregated Log and parse it

    data = {}
    for line in open(in_aggregated,"r").read().splitlines():

        fields=line.split(",")
        resolver=fields[0]
        count=int(fields[1])
        
        clients=json_to_counter(fields[2])
        queries=json_to_counter(fields[3])
        servers=json_to_counter(fields[4])
        asns=json_to_counter(fields[5])


        # Update per domain data
        domain_query_info={"count":count, "clients":clients, "queries":queries, "servers":servers,\
                           "asns":asns}
        data[resolver]=domain_query_info

    data_resolver = []
    for resolver in data:
        n_domains = len(data[resolver]["queries"])
        n_clients = len(data[resolver]["clients"])
        n_domains = len(data[resolver]["queries"])
        n_asns =    len(data[resolver]["asns"])
        n_servers = len(data[resolver]["servers"])
        count = data[resolver]["count"]
        data_resolver.append ( [ resolver, count, n_clients, n_domains, n_servers,n_asns] )

    resolvers_df = pd.DataFrame(data_resolver, \
                   columns = ["resolver", "count","clients","queried_domains","returned_servers","returned_asns"] )

    anomalies=pd.read_csv(in_anomalies, sep=" ", dtype={'asn':'str'})

    merged = pd.merge(resolvers_df,anomalies, left_on="resolver", right_on="res", how="left"   )

    merged = merged [ ["resolver", "count", "clients", "queried_domains",\
                       "asn", "domain", "servers"] ]


    merged = merged.sort_values(["queried_domains", "asn"], ascending=False)


    merged = merged.fillna("-")
    merged = merged.groupby(["resolver","queried_domains","count","clients", "asn"])\
                    .apply(lambda r: pd.Series( {\
                                            "domains":":".join(set(r["domain"])), 
                                            "servers":":".join(set(r["servers"]))\
                                           }))
    
    merged = merged.sort_index(level = ["queried_domains"],ascending=False )

    merged.to_csv(out_final_report)


def json_to_counter(s):
    s_clean=s.replace('""','"').replace(";",",")[1:-1]
    return Counter( json.loads(s_clean))







