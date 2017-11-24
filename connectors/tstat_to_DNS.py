from core.connector import Connector
from datetime import timedelta, date
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row

# DNS Manipulations
class Tstat_To_DNS(Connector):

    # Class Information
    _name = "Tstat_To_DNS"
    _input_format="Tstat"
    _output_type="DNS"

    # Run the algorithm
    def get_DF(self, spark_context, sql_context):

        paths=[]

        days=0
        for single_date in daterange(self.input_date_start, self.input_date_end):
            paths.append( single_date.strftime("%Y_%m_%d") )
            days+=1

        paths_str = self.input_path + "/*/*/{" + ",".join(paths) + "}*/log_dns_complete"

        log=spark_context.textFile(paths_str)

        # Parse each line of the Tstat LOG to group entries for the same Req
        log_grouped = log.mapPartitions(emit_keys).groupByKey(numPartitions=days*24)

        # Parse Req
        log_mapped=log_grouped.mapPartitions(emit_tuples)

        schema = self.schema      
        df = sql_context.createDataFrame(log_mapped, schema=schema)

        return df


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)

def emit_keys(lines):

    # Parse Lines
    for line in lines:
        try:    
            c_ip, c_port, s_ip, s_port, time, entry, trans_id,\
            query, ttl, d_class, d_type, answer, flags=\
                                                          line.split(" ")[:13]

            if flags != "flags:13":

                    flags_parsed = int(flags, 16)
                    time_bin_minute = int(float(time)/ (60*1000)  )            

                    # Create aggregation key and value
                    key = (time_bin_minute, c_ip, c_port, s_ip, s_port, trans_id)
                    value = (time, entry, query, ttl, d_class, d_type, answer, flags_parsed)

                    # Emit tuples
                    yield (key, value)
        except:
            pass    


def emit_tuples(tuples):
    
    for tup in tuples:
        time_bin, c_ip, c_port, s_ip, s_port, trans_id = tup[0]

        values = tup[1]

        # Init Vars
        answers=[]
        types=[]
        ttls = []
        min_time = 2**64
        max_time = 0

        original_query = None
        original_type =None
        original_class =None
        first_query = None
        first_type = None
        first_class = None
        flags_req = None
        flags_resp = None

        resp_code = None

        # Iterate over entries
        for time_str, entry, query, ttl, d_class, d_type, answer, flags_parsed in values:
            # Get times
            time_float = float (time_str)
            if time_float < min_time:
                min_time = time_float
                first_query = query
                first_type = d_type
                first_class = d_class
            if time_float > max_time:
                max_time = time_float

            # Get Original Query from REQ entries
            if entry == "REQ":
                flags_req = flags_parsed
                original_query = query
                original_type = d_type
                original_class = d_class

            # Add entries
            if entry == "RESP":
                flags_resp = flags_parsed
                answers.append(answer)
                types.append(d_type)
                ttls.append(int(ttl))
                resp_code="NOERROR"
            if entry == "RESP_ERR":
                resp_code=answer

        # Use First Query in responses if Query Packet is not seen
        if original_query == None:
            original_query = first_query
            original_type = first_type
            original_class =first_class

        
        out_entry = Row ( req_time=min_time,
                          resp_time=max_time,
                          c_ip=c_ip,
                          c_port=int(c_port),
                          s_ip=s_ip,
                          s_port=int(s_port),
                          trans_id=trans_id,
                          query=original_query,
                          req_type=original_type,
                          req_class=original_class,
                          flags_req=flags_req, 
                          flags_resp=flags_resp,
                          resp_code=resp_code,
                          answers=answers,
                          answer_types=types,
                          answer_ttls=ttls)

        yield (out_entry)



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
 
 
