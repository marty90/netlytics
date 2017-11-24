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

ttl_granularity=5

def calc_anomalies (in_aggregated, out_anomalies, min_resolutions):

    # Open Aggregated Log and parse it
    data_per_domain = defaultdict(dict)
    data_general    = defaultdict(lambda: defaultdict(lambda: Counter()))
    first=True
    for line in open(in_aggregated,"r").read().splitlines():
        if first:
            first=False
        else:
            try:
                fields=line.split(",")
                query=fields[0]
                resolver=fields[1]
                count=int(fields[2])
                clen=json_to_counter(fields[3])
                nip =json_to_counter(fields[4])
                asn =json_to_counter(fields[5])
                ttl =json_to_counter(fields[6]) 

                # Update per domain data
                domain_query_info={"ttl":ttl, "clen":clen, "nip":nip, "asn":asn}
                data_per_domain[query][resolver]=domain_query_info

                # Update general data
                data_general[query]["clen"] += clen
                data_general[query]["nip"]  += nip
                data_general[query]["asn"]  += asn
                data_general[query]["ttl"]  += ttl
                data_general[query]["count"]["__count__"] += count
            except Exception as e:
                print("Error", e)

    considered_domains = { domain for domain in data_general if data_general[domain]["count"]["__count__"] > min_resolutions }

    # Find "normality"
    normality=defaultdict(dict)
    for domain in considered_domains:

        ttl_all_max=Counter()
        all_ttls=[]
        for resolver in data_per_domain[domain]:
            this_ttl=data_per_domain[domain][resolver]["ttl"]
            max_ttl = max([int(k) for k in this_ttl.keys()])
            max_ttl_rounded = math.ceil(float(max_ttl)/ttl_granularity)*ttl_granularity 
            ttl_all_max [max_ttl_rounded] += 1
            all_ttls.append(max_ttl_rounded)
        value, count = ttl_all_max.most_common()[0]
        normality[domain]["ttl"]={"normality":numpy.percentile(all_ttls,75) + 
                                                    1.5*(   numpy.percentile(all_ttls,75) 
                                                          - numpy.percentile(all_ttls,15)   )}

        #CNAME CHAIN -> GET MOST COMMON VALUEs -- MORE FREQUENT THAN ratio_anomaly_cname
        for feature in ("clen", "nip" , "asn"):
            samples = data_general[domain][feature].values()
            m = statistical_anomalies(list(samples))
            normality[domain][feature]=m

      
    # Find "anomalies"
    anomalies = defaultdict(lambda : [])
    for domain in considered_domains:
        for resolver in data_per_domain[domain]:

            # TTL Use boxplot rule
            ttl_normal=0
            ttl_no_normal=0 
            max_ttl=0
            for this_ttl in data_per_domain[domain][resolver]["ttl"]:
                if int(this_ttl) > normality[domain]["ttl"]["normality"]:
                    ttl_no_normal += data_per_domain[domain][resolver]["ttl"][this_ttl]
                    if int(this_ttl) > max_ttl:
                        max_ttl=int(this_ttl)
                else:
                    ttl_normal += data_per_domain[domain][resolver]["ttl"][this_ttl]
            if ttl_no_normal > 0 and ttl_normal == 0:
                anomalies [ resolver + "," + domain].append("ttl")


            # CLEN, NIP and ASN: find different from normality
            for feature in ("clen", "nip" , "asn"):

                times_normal = 0
                times_no_normal = 0
                values_no_normal=set()

                for this_value in data_per_domain[domain][resolver][feature]:
                    this_frequency_global = data_general   [domain]          [feature][this_value]
                    this_frequency_local =  data_per_domain[domain][resolver][feature][this_value]
                    if this_frequency_global < normality[domain][feature]:
                        times_no_normal+=this_frequency_local
                        values_no_normal.add(this_value)
                    else:
                        times_normal+=this_frequency_local
                if times_no_normal > 0 and times_normal==0:
                    anomalies [ resolver + "," + domain].append(feature)

    # Save on file
    fo=open(out_anomalies,"w")
    print ( "resolver,query,type", file=fo, sep=",")
    for key in anomalies:
        print (key, ":".join(anomalies[key]), file=fo, sep=",")


def json_to_counter(s):
    s_clean=s.replace('""','"').replace(";",",")[1:-1]
    return Counter( json.loads(s_clean))


# 3Q+1.5*IQR
def statistical_anomalies(s):
  s_sum = sum(s)
  s_sorted_norm = [ float(e)/s_sum for e in sorted (s, reverse=True) ]
  s_sorted = [ e for e in sorted (s, reverse=True) ] 


  # Get Q1
  cum_sum=0
  old_cum_sum=0
  for i,n in enumerate ( s_sorted_norm):
    cum_sum += n
    if cum_sum >= 1.0/4.0:
      q_1=i - 1 +  ( (1.0/4.0) - old_cum_sum ) / n
      break
    old_cum_sum=cum_sum

  # Get Q3
  cum_sum=0
  old_cum_sum=0
  for i,n in enumerate ( s_sorted_norm):
    cum_sum += n
    if cum_sum >= 3.0/4.0:
      q_3 = i - 1 +  ( (3.0/4.0) - old_cum_sum ) / n
      break
    old_cum_sum=cum_sum

  th = (q_3) + 1.5 * (q_3 - q_1) 

  rounding=floor

  if rounding (th) >= len(s_sorted):
    return 0
  else:
    return s_sorted[int(rounding(th))]



