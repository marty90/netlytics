#!/usr/bin/python3

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from datetime import timedelta, date
import argparse
import importlib
import json
import os
import zipfile


def main():



    # Configure argparse    
    parser = argparse.ArgumentParser(description='NetLytics Job')

    parser.add_argument('--input_path', metavar='input_path', type=str, 
                       help='Base Log Files Input Path')   
                       
    parser.add_argument('--output_path', metavar='output_path', type=str, 
                       help='Directory where the output of the algo is stored')
                       
    parser.add_argument('--connector', metavar='connector', type=str, 
                        help='Connector class name')  

    parser.add_argument('--algo', metavar='algo', type=str, 
                        help='Algorithm to run')  

    parser.add_argument('--params', metavar='params', type=str, default = "{}",
                        help='Parameters to be given to the Algo, in Json')  

    parser.add_argument('--start_day', metavar='start_day', type=str, 
                        help='Start day for analysis, format YYYY_MM_DD')

    parser.add_argument('--end_day', metavar='end_day', type=str, 
                        help='End day for analysis, format YYYY_MM_DD')

    parser.add_argument('--temp_dir_local', metavar='temp_dir_local', type=str, default = "/tmp", 
                        help='Directory where to store intermediate files')

    parser.add_argument('--temp_dir_HDFS', metavar='temp_dir_HDFS', type=str, default = "tmp", 
                        help='Directory on HDFS where to store intermediate files')

    parser.add_argument('--persistent_dir_local', metavar='persistent_dir_local', type=str, default = "", 
                        help='Directory where to store persistent algorithm data (local)')

    parser.add_argument('--persistent_dir_HDFS', metavar='persistent_dir_HDFS', type=str, default = "", 
                        help='Directory where to store persistent algorithm data (HDFS)')

    # Get parameters
    args = vars(parser.parse_args())
    input_path=args["input_path"]
    output_path=args["output_path"]
    connector=args["connector"]
    algo=args["algo"]
    params=args["params"]
    start_day=args["start_day"]
    end_day=args["end_day"]
    temp_dir_local = args["temp_dir_local"]
    temp_dir_HDFS = args["temp_dir_HDFS"]
    persistent_dir_local= args["persistent_dir_local"]
    persistent_dir_HDFS= args["persistent_dir_HDFS"]

    # Get path of NetLytics
    base_path = os.path.dirname(os.path.realpath(__file__))
  
    # Create Spark Context 
    conf = (SparkConf()
             .setAppName("NetLytics Job")
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

    # Create Algo
    algo_module = my_import(algo,sc)
    algo_instance = algo_module(dataset, output_path,
                                temp_dir_local, temp_dir_HDFS,
                                persistent_dir_local, persistent_dir_HDFS)

    # Insert custom parameters
    params_parsed = json.loads(params)
    for p in params_parsed:
        algo_instance.parameters[p] = params_parsed[p]
    
    # Create output_path if not existing
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # Run it
    algo_instance.run()


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






