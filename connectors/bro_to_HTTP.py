from core.connector import Connector
from datetime import timedelta, date
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
import socket


class Bro_To_HTTP(Connector):

    # Class Information
    _name = "Bro_To_HTTP"
    _input_format="Bro"
    _output_type="HTTP"

    # Run the algorithm
    def get_DF(self, spark_context, sql_context):

        paths=[]

        days=0
        for single_date in daterange(self.input_date_start, self.input_date_end):
            paths.append( single_date.strftime("%Y-%m-%d") )
            days+=1

        paths_str = self.input_path + "/{" + ",".join(paths) + "}/http.*.log.gz"

        log=spark_context.textFile(paths_str)

        log_filtered = log.filter(lambda line: line[0] != "#")
      
        log_mapped = log_filtered.map(mapLine).repartition(days*24)

        schema = self.schema      
        df = sql_context.createDataFrame(log_mapped, schema = schema)

        return df


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)


def mapLine(line):
    
    fields = line.split("\t")

    min_time = float(fields[0])
    max_time = float(fields[0])

    c_ip     = fields[2]
    c_port   = int(fields[3])
    s_ip     = fields[4]
    s_port   = int(fields[5])

    method     = fields[7] if fields[7] != "-" else None
    host       = fields[8] if fields[8] != "-" else None
    path       = fields[9] if fields[9] != "-" else None
    referrer   = fields[10] if fields[10] != "-" else None
    user_agent = fields[12] if fields[12] != "-" else None
    request_body_len = int(fields[13]) if fields[13] != "-" else None
    req_cookie = None

    status_code       = int(fields[15]) if fields[15]!="-" else None
    response_body_len = int(fields[14]) if fields[14]!= "-" else None
    content_type      = fields[28] if fields[28] != "-" else None
    server = None
    content_range = None
    set_cookie = None

    out_entry = Row ( req_time=min_time,
                      resp_time=max_time,
                      c_ip=c_ip,
                      c_port=int(c_port),
                      s_ip=s_ip,
                      s_port=int(s_port),
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





 
