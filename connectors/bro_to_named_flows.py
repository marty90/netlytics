from core.connector import Connector
from datetime import timedelta, date
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
import socket


class Bro_To_Named_Flows(Connector):

    # Class Information
    _name = "Bro_To_Named_Flows"
    _input_format="Bro"
    _output_type="NamedFlows"

    # Run the algorithm
    def get_DF(self, spark_context, sql_context):

        paths=[]

        days=0
        for single_date in daterange(self.input_date_start, self.input_date_end):
            paths.append( single_date.strftime("%Y-%m-%d") )
            days+=1

        paths_conn = self.input_path + "/{" + ",".join(paths) + "}/conn.*.log.gz"
        paths_http = self.input_path + "/{" + ",".join(paths) + "}/http.*.log.gz"
        paths_ssl = self.input_path + "/{" + ",".join(paths) + "}/ssl.*.log.gz"

        log_conn_base=spark_context.textFile(paths_conn)
        log_conn = log_conn_base.filter(lambda line: line[0] != "#").map(mapConn).repartition(days*24)


        log_http_base=spark_context.textFile(paths_http)
        log_http = log_http_base.filter(lambda line: line[0] != "#").map(mapHTTP)\
                       .distinct().groupByKey().repartition(days*24)

        log_ssl_base=spark_context.textFile(paths_ssl)
        log_ssl = log_ssl_base.filter(lambda line: line[0] != "#").map(mapSSL)\
                     .distinct().groupByKey().repartition(days*24)

        log_names = log_http.union(log_ssl)

        joined = log_conn.join(log_names)

        log_named_flows = joined.map(mapLine)

        schema = self.schema      
        df = sql_context.createDataFrame(log_named_flows, schema = schema)

        return df


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)


def mapConn(line):

    fields = line.split("\t")

    time_start = float(fields[0])
    if fields[8] != "-":
        delta = float(fields[8])
    else:
        delta = 0.0
    time_end = time_start + delta

    c_ip = fields[2]
    c_port = int(fields[3])

    s_ip = fields[4]
    s_port = int(fields[5])

    c_bytes = int(fields[9]) if fields[9] != "-" else 0
    s_bytes = int(fields[10]) if fields[10] != "-" else 0

    c_pkt = int(fields[16]) if fields[16] != "-" else 0
    s_pkt = int(fields[18]) if fields[18] != "-" else 0

    conn_id = fields[1]

    tup = (conn_id,  (time_start, time_end, c_ip, c_port, s_ip, s_port, c_bytes, s_bytes, c_pkt, s_pkt)  )

    return tup

def mapHTTP(line):

    fields = line.split("\t")

    conn_id = fields[1]

    name = fields[8]

    return (conn_id, name)

def mapSSL(line):

    fields = line.split("\t")

    conn_id = fields[1]
    s_ip = fields[4]

    name = fields[9]

    if name == "-":
        name = s_ip

    return (conn_id, name)

def mapLine(tup):
    
    conn_id, ( (time_start, time_end, c_ip, c_port, s_ip, s_port, c_bytes, s_bytes, c_pkt, s_pkt), it) = tup

    name = None
    for n in it:
        name = n

    row = Row(  time_start=time_start,
                time_end=time_end,
                c_ip=c_ip,
                c_port=c_port,
                s_ip=s_ip,
                s_port=s_port,
                c_pkt=c_pkt,
                c_bytes=c_bytes,
                s_pkt=s_pkt,
                s_bytes=s_bytes,
                name=name)

    return row



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





 
