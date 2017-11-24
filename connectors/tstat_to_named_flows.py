from core.connector import Connector
from datetime import timedelta, date
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row

class Tstat_To_Named_Flows(Connector):

    # Class Information
    _name = "Tstat_To_Named_Flows"
    _input_format="Tstat"
    _output_type="NamedFlows"

    # Run the algorithm
    def get_DF(self, spark_context, sql_context):

        paths=[]

        days=0
        for single_date in daterange(self.input_date_start, self.input_date_end):
            paths.append( single_date.strftime("%Y_%m_%d") )
            days+=1

        paths_str = self.input_path + "/*/*/{" + ",".join(paths) + "}*/log_tcp_complete*"

        log=spark_context.textFile(paths_str)

        log_mapped = log.filter(filterLine).map(mapLine)

        schema = self.schema
        df = sql_context.createDataFrame(log_mapped, schema=schema)     

        return df

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)

def filterLine(line):
    splitted=line.split()
    try:
        time=float(splitted[28])
        http_hostname=splitted[130]
    except:
        return False
    return True
    
    
def mapLine(line):

    fields=line.split()
    
    time_start = float(fields[28])  / 1000
    time_end = float(fields[29])  / 1000

    c_ip    = fields[0]
    c_port  = int(fields[1])

    s_ip    = fields[14]
    s_port  = int(fields[15])

    c_pkt   = int(fields [2])
    c_bytes = int(fields [8])

    s_pkt   = int(fields [16])
    s_bytes = int(fields [22])

    fqdn    = fields [126]
    sni     = fields [115]
    host    = fields [130]

    name = s_ip
    if fqdn != "-":
        name = fqdn
    if sni != "-":
        name = sni    
    if host != "-":
        name = host  

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




