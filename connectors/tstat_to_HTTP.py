from core.connector import Connector
from datetime import timedelta, date
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row

class Tstat_To_HTTP(Connector):

    # Class Information
    _name = "Tstat_To_HTTP"
    _input_format="Tstat"
    _output_type="HTTP"

    # Run the algorithm
    def get_DF(self, spark_context, sql_context):

        paths=[]

        days=0
        for single_date in daterange(self.input_date_start, self.input_date_end):
            paths.append( single_date.strftime("%Y_%m_%d") )
            days+=1

        paths_str = self.input_path + "/*/*/{" + ",".join(paths) + "}*/log_http_complete*"

        log=spark_context.textFile(paths_str)

        log_mapped = log.filter(filterLine).map(mapLine).repartition(days*24)

        schema = self.schema
        df = sql_context.createDataFrame(log_mapped, schema=schema)     

        return df

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)

def filterLine(line):
    splitted=line.split("\t")
    try:
        time=float(splitted[4])
        cookie=splitted[12]
    except:
        return False
    return splitted[5] != "HTTP"
    
    
def mapLine(line):

    fields=line.split("\t")
    
    min_time = float(fields[4])
    max_time = float(fields[4])

    c_ip     = fields[0]
    c_port   = int(fields[1])
    s_ip     = fields[2]
    s_port   = int(fields[3])

    method     = fields[5] if fields[5] != "-" else None
    host       = fields[6] if fields[6] != "-" else None
    path       = fields[8] if fields[8] != "-" else None
    referrer   = fields[9] if fields[9] != "-" else None
    user_agent = fields[10] if fields[10] != "-" else None
    req_cookie = fields[11] if fields[11] != "-" else None
    request_body_len = None


    status_code       =  None
    response_body_len =  None
    content_type      =  None
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




