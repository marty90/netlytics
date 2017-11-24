from core.connector import Connector
from datetime import timedelta, date
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from datetime import datetime
import socket
import re

class Squid_To_HTTP(Connector):

    # Class Information
    _name = "Squid_To_HTTP"
    _input_format="Squid"
    _output_type="HTTP"

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
        min_time = float(fields[0])
        delta    = float(fields[1])/1000
        max_time = min_time + delta

        c_ip     = fields[2]
        c_port   = None
        s_ip     = fields[8].split("/")[1] if "/" in fields[8] else None

        method     = fields[5] if fields[5] != "-" else None

        if method == "CONNECT":
            s_port = int(fields[6].split(":")[1]) if ":" in fields[6] else None
            host   = fields[6].split(":")[0] if ":" in fields[6] else None
            path   = None
        elif "/" in fields[6]:
            host_long = fields[6].split("/")[2]
            if ":" in host_long:
                s_port = int(host_long.split(":")[1])
                host   = host_long.split(":")[0]
            else:
                s_port = 80
                host = host_long
            path = "/" + "/".join( fields[6].split("/")[3:] )
        else:
            s_port=host=path=None

        referrer   = None
        user_agent =  None
        request_body_len =  None
        req_cookie = None

        status_code       = int(fields[3].split("/")[1]) if "/" in fields[3] else None
        response_body_len = int(fields[4]) if fields[4]!= "-" else None
        content_type      = fields[9] if fields[9] != "-" else None
        server = None
        content_range = None
        set_cookie = None

        

    # Parse log combined
    else:
        fields=parse_line(line_clean)

        
        min_time = float(datetime.strptime( fields[3], "[%d/%b/%Y:%H:%M:%S" ).strftime('%s'))
        max_time = min_time

        c_ip     = fields[0]
        c_port   = None
        s_ip     = None

        method     = fields[5].split(" ")[0] if fields[5] != "-" else None
        url = fields[5].split(" ")[1]

        if method == "CONNECT":

            s_port = int(url.split(":")[1]) if ":" in url else None
            host   = url.split(":")[0] if ":" in url else url
            path   = None
        elif "/" in url:
            host_long = url.split("/")[2]
            if ":" in host_long:
                s_port = int(host_long.split(":")[1])
                host   = host_long.split(":")[0]
            else:
                s_port = 80
                host = host_long
            path = "/" + "/".join( url.split("/")[3:] )
        else:
            s_port=host=path=None

        referrer   = fields[8]
        user_agent =  fields[9]
        request_body_len =  None
        req_cookie = None

        status_code       = int(fields[6]) if fields[6]!="-" else None
        response_body_len = int(fields[7]) if fields[7]!="-" else None
        content_type      = None
        server = None
        content_range = None
        set_cookie = None


    out_entry = Row ( req_time=min_time,
      resp_time=max_time,
      c_ip=c_ip,
      c_port=c_port,
      s_ip=s_ip,
      s_port=s_port,
      method     = method,
      host       = host,
      path       = path,
      referrer   = referrer,
      user_agent = user_agent,
      request_body_len = request_body_len,
      req_cookie = req_cookie,
      status_code       = status_code,
      response_body_len = response_body_len,
      content_type      = content_type,
      server = server,
      content_range = content_range,
      set_cookie = set_cookie
    )
  
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



 
