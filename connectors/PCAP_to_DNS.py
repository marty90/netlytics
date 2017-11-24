from __future__ import print_function
from core.connector import Connector
from datetime import timedelta, date
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
import socket
from dateutil.parser import parse
import datetime
from scapy.layers.l2 import Ether
from scapy.layers.inet import IP, UDP
from scapy.layers.dns import *
from scapy.data import IP_PROTOS
from scapy.utils import PcapReader
from collections import Counter

import time
import datetime
import traceback
import sys
import tempfile
import glob
import os
import codecs
import shutil
from cStringIO import StringIO


Ether.payload_guess = [({"type": 0x800}, IP)]
IP.payload_guess = [({"frag": 0, "proto": 0x11}, UDP)]
UDP.payload_guess = [({"dport": 53}, DNS), ({"sport": 53}, DNS)]



class PCAP_To_DNS(Connector):

    # Class Information
    _name = "PCAP_To_DNS"
    _input_format="PCAP"
    _output_type="DNS"

    # Run the algorithm
    def get_DF(self, spark_context, sql_context):

        paths=[]

        days=0
        for single_date in daterange(self.input_date_start, self.input_date_end):
            paths.append( single_date.strftime("%Y%m%d") )
            days+=1

        paths_str = self.input_path + "/*.{" + ",".join(paths) + "}.*.*"
        #paths_str = self.input_path + "/*.pcap.gz"

        log=spark_context.binaryFiles(paths_str)

        log_mapped = log.flatMap(parse_file)
      


        schema = self.schema
        df = sql_context.createDataFrame(log_mapped, schema=schema)

        return df



class TSTAT_PCAP_To_DNS(Connector):

    # Class Information
    _name = "TSTAT_PCAP_To_DNS"
    _input_format="TSTAT_PCAP"
    _output_type="DNS"

    # Run the algorithm
    def get_DF(self, spark_context, sql_context):

        paths=[]

        days=0
        for single_date in daterange(self.input_date_start, self.input_date_end):
            paths.append( single_date.strftime("%Y_%m_%d") )
            days+=1

        paths_str = self.input_path + "/*/*/{" + ",".join(paths) + "}*/*/udp_dns.pcap*"

        log=spark_context.binaryFiles(paths_str)

        log_mapped = log.flatMap(parse_file)
      


        schema = self.schema
        df = sql_context.createDataFrame(log_mapped, schema=schema)

        return df


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)


def parse_file( tup  ):

    path, content = tup

    ftmp = tempfile.NamedTemporaryFile(delete=True)
    ftmp.write(content)
    ftmp.flush()

    pcap_reader = PcapReader(ftmp.name)
    count=0
    for packet in pcap_reader:
        count+=1
        try:
            row = parse_packet(packet)
            if row is not None:
                yield row
        except:
            pass

    ftmp.close()
    


def parse_packet(packet):
    if DNS in packet and packet[DNS].qr == 1:
        r = StringIO()
        
        
        min_time = max_time = float(packet.time)

        s_ip     = packet[IP].src
        s_port   = int(packet[UDP].sport)
        c_ip     = packet[IP].dst
        c_port   = int(packet[UDP].dport)
        trans_id = int(packet[DNS].id)
        query = repr_data(packet[DNSQR].qname)
        req_type = define_type(packet[DNSQR].qtype)
        req_class = define_class(packet[DNSQR].qclass)
        flags_req = None
  
        flag_AA = 1 if packet[DNS].aa == 1  else 0
        flag_TC = 1 if packet[DNS].tc == 1  else 0
        flag_RD = 1 if packet[DNS].rd == 1  else 0
        flag_RA = 1 if packet[DNS].rd == 1  else 0
        flags_resp = 0 + 0 + (flag_RA << 2) + (flag_RD << 3) + (flag_TC << 4) + (flag_AA << 5)

        resp_code = define_rcode(packet[DNS].rcode)

        answers      = parse_ans(packet)  # DANS
        answer_ttls  = parse_anttls(packet)  # DANTTLS
        answer_types = parse_antypes(packet) # DANTYPES

       

        out_entry = Row ( req_time=min_time,
                          resp_time=max_time,
                          c_ip=c_ip,
                          c_port=c_port,
                          s_ip=s_ip,
                          s_port=s_port,
                          trans_id=trans_id,
                          query=query,
                          req_type=req_type,
                          req_class=req_class,
                          flags_req=flags_req, 
                          flags_resp=flags_resp,
                          resp_code=resp_code,
                          answers=answers,
                          answer_types=answer_types,
                          answer_ttls=answer_ttls)

        return out_entry

    else:
        return None

def define_protocol(proto):
    if proto == IP_PROTOS.tcp:
        return "tcp"
    elif proto == IP_PROTOS.udp:
        return "udp"
    else:
        return ""


def count_labels(qname):
    labels = qname.split(".")
    labels = filter(None, labels)
    return len(labels)


dnsrcodes = {
    0: "NOERROR",
    1: "FORMERR",
    2: "SERVFAIL",
    3: "NXDOMAIN",
    4: "NOTIMP",
    5: "REFUSED",
    6: "YXDOMAIN",
    7: "YXRRSET",
    8: "NXRRSET",
    9: "NOTAUTH",
    10: "NOTZONE",
    16: "BADVERS/BADSIG",
    17: "BADKEY",
    18: "BADTIME",
    19: "BADMODE",
    20: "BADNAME",
    21: "BADALG",
    22: "BADTRUNC",
    23: "BADCOOKIE",
    65535: "RESERVED"
}


def define_rcode(rcode):
    res = dnsrcodes.get(rcode)
    if res is None:
        if 3841 <= rcode <= 4095:
            return "RESERVED"
        else:
            return "UNASSIGNED"
        
    return res

dnstypes.update({
    52: "TLSA",
    53: "SMIMEA",
    59: "CDS",
    60: "CDNSKEY",
    61: "OPENPGPKEY",
    62: "CSYNC",
    104: "NID",
    105: "L32",
    106: "L64",
    107: "LP",
    108: "EUI48",
    109: "EUI64",
    251: "IXFR",
    252: "AXFR",
    253: "MAILB",
    254: "MAILA",
    255: "*",
    256: "URI",
    257: "CAA",
    258: "AVC",
    65535: "RESERVED"
})


def define_type(tcode):
    dns_type = dnstypes.get(tcode)
    if dns_type is None:
        if 65280 <= tcode <= 65534:
            return "PRIVATE_USE"
        else:
            return "UNASSIGNED"
    
    return dns_type


dnsclasses.update({
    0: "RESERVED",
    254: "NONE",
    65535: "RESERVED"
})


def define_class(ccode):
    dns_class = dnsclasses.get(ccode)
    if dns_class is None:
        if 65280 <= ccode <= 65534:
            return "PRIVATE_USE"
        else:
            return "UNASSIGNED"
    
    return dns_class

def repr_data(data):
    res = repr(data)[1:-1]
    return res.replace('"', '""')


def parse_ans(p):
    ans=[]
    for i in range(p[DNS].ancount):
        
        ans.append(repr_data(p.an[i].rdata))
        add = True
    
    return ans


def parse_anttls(p):
    ttls=[]
    for i in range(p[DNS].ancount):
        ttls.append(int(p.an[i].ttl))
    
    return ttls


def parse_antypes(p):
    types=[]
    for i in range(p[DNS].ancount):
        types.append(define_type(p.an[i].type))
    
    return types



