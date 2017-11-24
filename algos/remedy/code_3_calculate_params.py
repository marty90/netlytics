#!/usr/bin/python

import json
import sys
import pyasn
from collections import Counter
from pyspark import SparkConf, SparkContext
import numpy as np 


def emit_tuples_SLD_ASN(lines,anomalies):

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

            # Keep only NOERROR responses and recursive queries
            if DRES == "NOERROR" and DFRD == "1" and DFRA == "1":
            
                # Get Number of CNAMEs and Server IP addresses
                records=str(DANS).split('|-><-|')
                sip=set()
                clen=0
                nip=0
                for record in records:
                    if is_valid_ipv4(record):
                        sip.add(record)
                        nip+=1
                    else:
                        clen+=1
                
                # Continue only if at least one IP address has been returned
                if nip > 0:      
                    # Get the list of ASNs from t server IPs
                    asns=[]
                    for ip in sip:
                        try:
                            this_asn = str(asndb.lookup(ip)[0])
                            if this_asn == "None":
                                this_asn = ".".join(ip.split(".")[0:2]  ) + ".0.0"
                            if ip.startswith("127.0."):
                                this_asn=ip
                        except Exception as e:
                            this_asn=ip
                        asns.append(this_asn)

                    # Emit a tuple for each couple Query ASN
                    for asn in asns:
          
                        # Only if it is not anomalous
                        lookup = str(DST) + " " + str(DQ).lower()  
                        if lookup not in anomalies:
                            SLD = getGood2LD(str(DQ).lower())
                            tup = (asn, SLD)
                            yield tup



def emit_tuples_SLD_COUNT(lines, anomalies):
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

            # Keep only NOERROR responses and recursive queries
            if DRES == "NOERROR" and DFRD == "1" and DFRA == "1":
            
                # Get Number of CNAMEs and Server IP addresses
                records=str(DANS).split('|-><-|')
                sip=set()
                clen=0
                nip=0
                for record in records:
                    if is_valid_ipv4(record):
                        sip.add(record)
                        nip+=1
                    else:
                        clen+=1
                
                # Continue only if at least one IP address has been returned
                if nip > 0:      
                    # Get the list of ASNs from t server IPs
                    asns=[]
                    for ip in sip:
                        try:
                            this_asn = str(asndb.lookup(ip)[0])
                        except Exception as e:
                            this_asn=ip
                        asns.append(this_asn)

                    # Emit a tuple for each couple Query ASN
                    for asn in asns:
          
                        # Only if it is not anomalous
                        lookup = str(DST) + " " + str(DQ).lower()  
                        if lookup not in anomalies:
                            SLD = getGood2LD(str(DQ).lower())
                            tup = (asn + " " + SLD, 1)
                            yield tup


# Reduce is just merging the two sets
def reduce_tuples(tup1,tup2):
    n1, clen1,nip1,asn1,ttl1,sip1=tup1
    n2, clen2,nip2,asn2,ttl2,sip2=tup2

    return (n1+n2, clen1+clen2, \
                   nip1+nip2,   \
                   asn1+asn2,   \
                   ttl1+ttl2,   \
                   sip1+sip2  )
                   
# In the end, just print the Counter in a Pandas friendly format
def final_map(tup):
    (res,fqdn), (n, clen,nip,asn,ttl,sip) = tup

    n_str=str(n)
    clen_str='"' + json.dumps(clen).replace('"','""').replace(",",";")+ '"'
    nip_str= '"' + json.dumps(nip).replace('"','""').replace(",",";")+ '"'
    asn_str= '"' + json.dumps(asn).replace('"','""').replace(",",";")+ '"'
    ttl_str= '"' + json.dumps(ttl).replace('"','""').replace(",",";")+ '"'
    sip_str= '"' + json.dumps(sip).replace('"','""').replace(",",";")+ '"'

    return ",".join([fqdn,res,n_str,clen_str,nip_str,asn_str,ttl_str,sip_str])

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






