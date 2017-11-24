from core.connector import Connector
from datetime import timedelta, date
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from datetime import datetime
import socket
import re

class Squid_To_Named_Flows(Connector):

    # Class Information
    _name = "Squid_To_Named_Flows"
    _input_format="Squid"
    _output_type="NamedFlows"

    # Run the algorithm
    def get_DF(self, spark_context, sql_context):

        paths=[]

        days=0
        for single_date in daterange(self.input_date_start, self.input_date_end):
            paths.append( single_date.strftime("%Y%m%d") )
            days+=1

        paths_str = self.input_path + "/access.log-{" + ",".join(paths) + "}"

        log=spark_context.textFile(paths_str)
      
        log_mapped = log.map(mapLine).repartition(days*24)

        schema = self.schema      
        df = sql_context.createDataFrame(log_mapped, schema = schema)

        return df


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)


def mapLine(line):
    line_clean = re.sub(' +',' ',line)

    fields = line_clean.split(" ")

    # Parse log squid
    if len(fields) == 10:
        time_start = float(fields[0])
        delta    = float(fields[1])/1000
        time_end = time_start + delta

        c_ip     = fields[2]
        c_port   = None
        s_ip     = fields[8].split("/")[1] if "/" in fields[8] else None

        method     = fields[5] if fields[5] != "-" else None

        if method == "CONNECT":
            s_port = int(fields[6].split(":")[1]) if ":" in fields[6] else None
            name   = fields[6].split(":")[0] if ":" in fields[6] else None
        elif "/" in fields[6]:
            host_long = fields[6].split("/")[2]
            if ":" in host_long:
                s_port = int(host_long.split(":")[1])
                name   = host_long.split(":")[0]
            else:
                s_port = 80
                name = host_long
            path = "/" + "/".join( fields[6].split("/")[3:] )
        else:
            s_port=host=None


        c_pkt   = None
        c_bytes = None
        s_pkt   = None

        s_bytes = int(fields[4]) if fields[4]!= "-" else None
        
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

def parse_line(line):
    fields = []
    current_field=""
    in_quote=False
    for c in line:
        if not in_quote and c == " ":
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



 
