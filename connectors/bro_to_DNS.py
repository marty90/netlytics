from core.connector import Connector
from datetime import timedelta, date
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
import socket

class Bro_To_DNS(Connector):

    # Class Information
    _name = "Bro_To_DNS"
    _input_format="Bro"
    _output_type="DNS"

    # Run the algorithm
    def get_DF(self, spark_context, sql_context):

        paths=[]

        days=0
        for single_date in daterange(self.input_date_start, self.input_date_end):
            paths.append( single_date.strftime("%Y-%m-%d") )
            days+=1

        paths_str = self.input_path + "/{" + ",".join(paths) + "}/dns.*.log.gz"

        log=spark_context.textFile(paths_str)

        log_filtered = log.filter(lambda line: line[0] != "#")
      
        log_mapped = log_filtered.map(mapLine).repartition(days*24)

        schema = self.schema
        df = sql_context.createDataFrame(log_mapped, schema=schema)

        return df


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)


def mapLine(line):
    
    fields = line.split("\t")

    min_time = float(fields[0])
    if fields[8] != "-":
        delta = float(fields[8])
    else:
        delta = 0.0
    max_time = min_time + delta

    c_ip     = fields[2]
    c_port   = int(fields[3])
    s_ip     = fields[4]
    s_port   = int(fields[5])
    trans_id = fields[7]
    query = fields[9] if fields[9] != "-" else None
    req_type = fields[13] if fields[13] != "-" else None
    req_class = fields[11] if fields[11] != "-" else None
    flags_req = None
    
    flag_AA = 1 if fields[16] == "T" else 0
    flag_TC = 1 if fields[17] == "T" else 0
    flag_RD = 1 if fields[18] == "T" else 0
    flag_RA = 1 if fields[19] == "T" else 0
    flags_resp = 0 + 0 + (flag_RA << 2) + (flag_RD << 3) + (flag_TC << 4) + (flag_AA << 5)

    resp_code = fields[15] if fields[15] != "-" else None

    if fields[21] == "-":
        answers=[]
    else:
        answers=fields[21].split(",")

    if fields[22] == "-":
        answer_ttls=[]
    else:
        answer_ttls=[ int(float(s)) for s in fields[22].split(",") ]

    if len (answers) == 0:
        answer_types = []
    elif len (answers) == 1:
        answer_types = [req_type]
    elif len (answers) > 1:
        answer_types = []
        nb_requested_type = 0
        for a in answers:
            if is_valid_ip(a):
                nb_requested_type+=1
        if fields[13] not in {"-","*",""} :
            answer_types +=  ["CNAME"]* (len (answers) - nb_requested_type)  
            answer_types +=  [req_type]*  nb_requested_type  

    out_entry = Row ( req_time=min_time,
                      resp_time=max_time,
                      c_ip=c_ip,
                      c_port=int(c_port),
                      s_ip=s_ip,
                      s_port=int(s_port),
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
 
def is_valid_ipv6(address):
    try:
        socket.inet_pton(socket.AF_INET6, address)
    except socket.error:  # not a valid address
        return False
    return True

def is_valid_ip (address):
    return is_valid_ipv6(address) or is_valid_ipv4(address)





 
