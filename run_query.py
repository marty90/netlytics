#!/usr/bin/python3

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from datetime import timedelta, date
import argparse
import importlib
import json
import os.path
import zipfile


def main():



    # Configure argparse    
    parser = argparse.ArgumentParser(description='NetLytics Job')

    parser.add_argument('--input_path', metavar='input_path', type=str, 
                       help='Base Log Files Input Path')   
                       
    parser.add_argument('--output_file_local', metavar='output_file_local', type=str, default= "",
                       help='File where the resulting table is saved (locally). Cannot be specified together with output_file_HDFS')

    parser.add_argument('--output_file_HDFS', metavar='output_file_HDFS', type=str, default= "",
                       help='File where the resulting table is saved (HDFS).  Cannot be specified together with output_file_local')
                       
    parser.add_argument('--query', metavar='query', type=str, 
                       help='SQL Query to exectute. Use "netlytics" as SQL table name') 

    parser.add_argument('--connector', metavar='connector', type=str, 
                        help='Connector class name')  

    parser.add_argument('--start_day', metavar='start_day', type=str, 
                        help='Start day for analysis, format YYYY_MM_DD')

    parser.add_argument('--end_day', metavar='end_day', type=str, 
                        help='End day for analysis, format YYYY_MM_DD')

    

    # Get parameters
    args = vars(parser.parse_args())
    input_path=args["input_path"]
    query=args["query"]
    output_file_local=args["output_file_local"]
    output_file_HDFS=args["output_file_HDFS"]
    connector=args["connector"]
    start_day=args["start_day"]
    end_day=args["end_day"]

    # Check Output
    if output_file_local == "" and output_file_HDFS == "":
        print ("At least one between <output_file_local> and <output_file_HDFS> must be specified.")
        return

    if output_file_local != "" and output_file_HDFS != "":
        print ("Cannot specify both <output_file_local> and <output_file_HDFS>.")    
        return

    # Get path of NetLytics
    base_path = os.path.dirname(os.path.realpath(__file__))

    # Create Spark Context 
    conf = (SparkConf()
             .setAppName("NetLytics SQL Query")
    )
    sc = SparkContext(conf = conf)
    spark = SparkSession(sc)

    # Ship code to executors
    ship_dir (base_path + "/algos",sc, base_path)
    ship_dir (base_path + "/core",sc, base_path)
    ship_dir (base_path + "/connectors",sc, base_path)

    # Find connector
    connector_module = my_import(connector,sc)

    # Parse dates
    y1,m1,d1 = start_day.split("_")
    date1 = date (int(y1),int(m1),int(d1))
    y2,m2,d2 = end_day.split("_")
    date2 = date (int(y2),int(m2),int(d2))

    # Instantiate connector
    connector_instance = connector_module(input_path,date1,date2 )
    
    # Get and enforce Schema
    output_type = connector_instance.output_type
    schema_file = base_path + "/schema/" + output_type + ".json"
    schema_json = json.load(open(schema_file,"r"))
    schema = StructType.fromJson(schema_json)
    connector_instance.set_schema (schema)

    # Get Dataset
    dataset = connector_instance.get_DF(sc,spark)
    dataset.createOrReplaceTempView("netlytics")

    # Execute Query
    result_df = spark.sql(query)

    # Save
    if output_file_HDFS != "":
        result_df.write.save(output_file_HDFS,"csv")
    elif output_file_local != "":
        df_pandas = result_df.toPandas()
        df_pandas.to_csv(output_file_local, encoding="raw_unicode_escape")





def my_import(name,sc):
    labels = name.split(".")
    base_name = ".".join(labels[:-1])
    module = importlib.import_module(base_name)
    my_class = getattr(module, labels[-1])
    return my_class

n=0
def ship_dir(path,sc, base_path):
    global n
    n+=1
    zipf = zipfile.ZipFile('/tmp/' + str(n)+ '.zip', 'w', zipfile.ZIP_DEFLATED)
    zipdir(path + '/', zipf, base_path)
    zipf.close()
    sc.addPyFile('/tmp/' + str(n)+ '.zip')

def zipdir(path, ziph, base_path):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file),  os.path.relpath(os.path.join(root, file),base_path )  )

main()






