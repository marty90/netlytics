from core.connector import Connector
from datetime import timedelta, date
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
import socket
from dateutil.parser import parse
import datetime

class Bind_To_DNS(Connector):

    # Class Information
    _name = "Bind_To_DNS"
    _input_format="Bind"
    _output_type="DNS"

    # Run the algorithm
    def get_DF(self, spark_context, sql_context):

        paths=[]

        days=0
        for single_date in daterange(self.input_date_start, self.input_date_end):
            paths.append( single_date.strftime("%Y%m%d") )
            days+=1

        paths_str = self.input_path + "/bind.log-{" + ",".join(paths) + "}"

        log=spark_context.textFile(paths_str)

        log_filtered = log.filter(getQueries)
      
        log_mapped = log_filtered.map(mapLine).repartition(days*24)

        schema = self.schema
        df = sql_context.createDataFrame(log_mapped, schema=schema)

        return df


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)


def getQueries(line):
  
    fields=line.split(" ")
    return fields[2] == "queries:"

def mapLine(line):
    
    fields = line.split(" ")

    time_str = fields[0] + " " + fields [1]

    min_time = max_time = (parse (time_str)- datetime.datetime(1970,1,1)).total_seconds()

    c_ip     = fields[5].split("#")[0]
    c_port   = int(fields[5].split("#")[1])
    s_ip     = None
    s_port   = None
    trans_id = None
    query = fields[8]
    req_type = fields[10] 
    req_class = fields[9]
    flags_req = None
   
    flags_resp = None

    resp_code = None

    answers=[]

    answer_ttls=[]

    answer_types = []
     

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





 
