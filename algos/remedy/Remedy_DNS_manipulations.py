from __future__ import print_function
from core.algo import Algo
# Import code
import algos.remedy.code_1_spark_aggregate_res_dom
import algos.remedy.code_2_find_anomalies_res_dom
import algos.remedy.code_4_spark_aggregate_res_asn
import algos.remedy.code_5_find_anomalies_res_asn
import algos.remedy.code_6_spark_aggregate_res
import algos.remedy.code_7_create_final_report
from shutil import copyfile

import numpy as np 
from collections import Counter
import pyasn
import json
from subprocess import call

hdfs_command="hdfs"

# DNS Manipulations
class RemedyDNSManipulations(Algo):

    # Class Information
    _name = "DNS_manipulations"
    _input_type="DNS"
    parameters={"min_resolutions":50, "ASN_VIEW": "ASN_VIEW_2017", "partitions": 500}

    # Run the algorithm
    def run(self):
        input_DF = self.input_DF
        
        # Add BGP view
        sc = input_DF.rdd.context
        ASN_VIEW = self.parameters["ASN_VIEW"]
        copyfile(ASN_VIEW, self.temp_dir_local + "/ASN_VIEW")
        sc.addFile(self.temp_dir_local + "/ASN_VIEW")

        # 1. Aggregate RESOLVER DOMAIN
        # Parse each line of the ATA DNS log file
        num_partitions= self.parameters["partitions"]
        log_mapped=input_DF.rdd.repartition(num_partitions)\
                               .mapPartitions(algos.remedy.code_1_spark_aggregate_res_dom.emit_tuples)\
                               .repartition(num_partitions)


        # Reduce tuples, aggregate by (resolver, domain)
        log_reduced=log_mapped.reduceByKey(algos.remedy.code_1_spark_aggregate_res_dom.reduce_tuples)

        # Put in final format
        log_final=log_reduced.map(algos.remedy.code_1_spark_aggregate_res_dom.final_map)

        # Save it
        call (hdfs_command + " dfs -rm -r " + self.temp_dir_HDFS + "/aggregated-res-dom.csv", shell=True)
        log_final.saveAsTextFile(self.temp_dir_HDFS + "/aggregated-res-dom.csv")
        call (hdfs_command + " dfs -getmerge " + \
                            self.temp_dir_HDFS + "/aggregated-res-dom.csv" + " " + \
                            self.temp_dir_local + "/aggregated-res-dom.csv" , shell=True)  


        # 2. Anomalies RESOLVER DOMAIN
        algos.remedy.code_2_find_anomalies_res_dom.calc_anomalies(\
                                                   self.temp_dir_local + "/aggregated-res-dom.csv",
                                                   self.temp_dir_local + "/anomalies-res-dom.csv",
                                                   self.parameters["min_resolutions"] )

        # 3. Calculate Params
        anomalies = { line.split(",")[0] + " " + line.split(",")[1]  \
                  for line in open(self.temp_dir_local + "/anomalies-res-dom.csv","r").read().splitlines()}

        # Parse each line of the ATA DNS log file
        f_SLD_ASN_couples = lambda p: emit_tuples_SLD_ASN (p,anomalies)
        SLD_ASN_couples=input_DF\
                        .rdd.repartition(num_partitions).mapPartitions(f_SLD_ASN_couples)\
                        .distinct()

        # Compute stat: SLD_ASN
        domains_per_ASN=SLD_ASN_couples.countByKey()
        samples = domains_per_ASN.values()
        percentiles = np.percentile (samples, [0,5,25,50,75,95,100])
        
        stats_SLD_ASN = zip([0,5,25,50,75,95,100],percentiles )
        
        # Compute stat: SLD_COUNT
        f_SLD_COUNT_couples = lambda p: emit_tuples_SLD_COUNT (p,anomalies)
        SLD_COUNT_couples=input_DF\
                         .rdd.repartition(num_partitions).mapPartitions(f_SLD_COUNT_couples)\
                         .countByKey()

        samples = SLD_COUNT_couples.values()
        percentiles = np.percentile (samples, [0,5,25,50,75,95,100])
        stats_SLD_COUNT = zip([0,5,25,50,75,95,100],percentiles )

        # Save on files
        params = {"SLD_ASN" : stats_SLD_ASN, "SLD_COUNT" : stats_SLD_COUNT}
        json.dump(params,open(self.temp_dir_local + "/params.json","w"))


        # 4. Aggregate Resolver ASN

        # Parse each line of the ATA DNS log file
        log_mapped=input_DF.rdd.repartition(num_partitions)\
                .mapPartitions(algos.remedy.code_4_spark_aggregate_res_asn.emit_tuples)

        # Reduce tuples, aggregate by (resolver, ASN)
        log_reduced=log_mapped.reduceByKey(algos.remedy.code_4_spark_aggregate_res_asn.reduce_tuples)

        # Put in final format
        log_final=log_reduced.map(algos.remedy.code_4_spark_aggregate_res_asn.final_map)

        # Save it
        call (hdfs_command + " dfs -rm -r " + self.temp_dir_HDFS + "/aggregated-res-asn.csv", shell=True)
        log_final.saveAsTextFile(self.temp_dir_HDFS + "/aggregated-res-asn.csv")
        call (hdfs_command + " dfs -getmerge " + \
                            self.temp_dir_HDFS + "/aggregated-res-asn.csv" + " " + \
                            self.temp_dir_local + "/aggregated-res-asn.csv" , shell=True)


        #5. Find anomalies res-asn
        algos.remedy.code_5_find_anomalies_res_asn.find_anomalies(\
            self.temp_dir_local + "/aggregated-res-asn.csv",\
            self.temp_dir_local + "/params.json",\
            self.temp_dir_local + "/anomalies-res-dom.csv",\
            self.temp_dir_local + "/anomalies-res-dom-asn.csv")


        # 6. Aggregate Res ASN

        # Parse each line of the ATA DNS log file
        log_mapped=input_DF.rdd.repartition(num_partitions)\
                           .mapPartitions(algos.remedy.code_6_spark_aggregate_res.emit_tuples)\
                           .repartition(num_partitions)

        # Reduce tuples, aggregate by (resolver)
        log_reduced=log_mapped.reduceByKey(algos.remedy.code_6_spark_aggregate_res.reduce_tuples)

        # Put in final format
        log_final=log_reduced.map(algos.remedy.code_6_spark_aggregate_res.final_map)

        # Save it
        call (hdfs_command + " dfs -rm -r " + self.temp_dir_HDFS + "/aggregated-res.csv", shell=True)
        log_final.saveAsTextFile(self.temp_dir_HDFS + "/aggregated-res.csv")
        call (hdfs_command + " dfs -getmerge " + \
                            self.temp_dir_HDFS + "/aggregated-res.csv" + " " + \
                            self.temp_dir_local + "/aggregated-res.csv" , shell=True) 


        # 7. Report
        algos.remedy.code_7_create_final_report.create_report(\
                                                  self.temp_dir_local + "/aggregated-res.csv",\
                                                  self.temp_dir_local + "/anomalies-res-dom-asn.csv",\
                                                  self.output_dir + "/final-report.csv")

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






