#!/usr/bin/python3

import pandas as pd
import requests
from collections import defaultdict
import json
import os
import sys

TOKEN="ee48f627-66e5-4a2f-8489-b94994a8e8fd"
HEADERS = {'Authorization': 'Bearer ' + TOKEN}


input_report=sys.argv[1]
cache_dir=sys.argv[2]
out_report = sys.argv[3]

CDN_DOMAINS = {"akamaiedge","edgekey","akamai","edgesuite", "akadns"}

def main():
    report = pd.read_csv(input_report)
    
    filtered_report=pd.DataFrame()
    for i, row in report.iterrows():
        domains = row.domains.split(":")
        row["hints"]="."
        if row.asn != "-":
            asn = row.asn

            legitimate_asns = set()
            found_CDN = True
            for domain in domains:
                domain_info = get_domain_info(domain,cache_dir)
                this_asns = { str(asn) for asn in set(domain_info["asns"] )}
                legitimate_asns |= this_asns
                is_this_on_CDN = False
                for alias in domain_info["aliases"]:
                    for label in alias.split("."):
                        if label in CDN_DOMAINS:
                            is_this_on_CDN=True
                if not is_this_on_CDN:
                    found_CDN=False

            if asn in legitimate_asns:
                print (row.resolver , row.asn, "not anomaly")
                row["hints"]+="Verified."
            if found_CDN:
                row["hints"]+="CDN."

        filtered_report = filtered_report.append(row)              

    filtered_report=filtered_report[["resolver",  "queried_domains", "count", "clients",\
                                      "asn", "domains", "servers", "hints"]]

    filtered_report.to_csv(out_report)

def get_domain_info (domain, cache_dir):
    
    domain = domain.strip(".")
    
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    
    file_path = os.path.join(cache_dir, domain)
    
    if os.path.isfile(file_path):
        #print (domain ,"found in cache" )
        return json.load(open(file_path,"r"))
        
    else:
        
        asns = set()
        visit_list = [ domain ]
        aliases = {domain}
        print ("Visiting", domain)
        
        while len (visit_list) > 0:
            try:
                this_domain = visit_list.pop()
                print ("    Inspecting", this_domain)
                url = "https://investigate.api.umbrella.com/dnsdb/name/a/" + this_domain + ".json"
                response = requests.get( url, headers=HEADERS)
                response_parsed = response.json()
                
                if "asns" in response_parsed["features"]:
                    print ("        Added", set ( response_parsed["features"]["asns"]), "<---------")
                    asns |= set ( response_parsed["features"]["asns"] )
                    
                for rrs_tf in response_parsed["rrs_tf"]:
                    for rr in rrs_tf["rrs"]:
                        if rr["type"] == "CNAME":
                            if not rr["rr"] in visit_list:
                                print ("        Added", rr["rr"])
                                visit_list.append(rr["rr"])
                                aliases.add(rr["rr"])
            except Exception as e:
                print ("Exception", e)   
        
        record = {"asns" : list(asns), "aliases":list(aliases)}
        json.dump(record, open(file_path,"w")) 
        return record


main()

