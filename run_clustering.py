#!/usr/bin/python3

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType
from datetime import timedelta, date
import argparse
import importlib
import json
import os
import zipfile
import core.manipulate_dataframe
from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.linalg import DenseVector
import operator

def main():



    # Configure argparse    
    parser = argparse.ArgumentParser(description='NetLytics Job')

    parser.add_argument('--connector', metavar='connector', type=str, 
                        help='Connector class name')  

    parser.add_argument('--input_path', metavar='input_path', type=str, 
                       help='Base Log Files Input Path')   

    parser.add_argument('--start_day', metavar='start_day', type=str, 
                        help='Start day for analysis, format YYYY_MM_DD')

    parser.add_argument('--end_day', metavar='end_day', type=str, 
                        help='End day for analysis, format YYYY_MM_DD')
                       
    parser.add_argument('--output_path', metavar='output_path', type=str, 
                       help='Path where to store resulting labeled Data Table')
                      
    parser.add_argument('--algo', metavar='algo', type=str, 
                        help='Clustering Algorithm to run')  

    parser.add_argument('--params', metavar='params', type=str, default = "{}",
                        help='Parameters to be given to the Clustering Algorithm, in Json')  

    parser.add_argument('--query', metavar='query', type=str, default = None,
                        help='Eventual SQL query to execute to preprocess the dataset')

    parser.add_argument('--numerical_features', metavar='numerical_features', type=str, default="",
                        help='Columns to use as numerical features, separated by comma')   

    parser.add_argument('--categorical_features', metavar='categorical_features', type=str, default="",
                        help='Columns to use as categorical features, separated by comma') 


    parser.add_argument("--normalize", action="store_true",
                        help="Normalize data before clustering")

    # Get parameters
    args = vars(parser.parse_args())
    input_path=args["input_path"]
    output_path=args["output_path"]
    connector=args["connector"]
    algo=args["algo"]
    params=args["params"]
    start_day=args["start_day"]
    end_day=args["end_day"]
    query=args["query"]
    numerical_features=args["numerical_features"].split(",") if args["numerical_features"] != "" else []
    categorical_features=args["categorical_features"].split(",") if args["categorical_features"] != "" else []
    normalize=args["normalize"]

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

    # Run clusertering
    manipulated_dataset = core.manipulate_dataframe.transform(dataset,spark,\
                                                sql_query = query,\
                                                numerical_features = numerical_features,
                                                categorical_features = categorical_features,
                                                normalize=normalize)

    clustering_algo_module = my_import(algo,sc)
    clustering_algo_instance = clustering_algo_module( json.loads(params) )

    prediction = clustering_algo_instance.run(manipulated_dataset)

    # Save Output
    def RowToStr(row):
        out_dict={}
        in_dict =row.asDict()
        for f in in_dict:
            if isinstance(in_dict[f], DenseVector):
                j = '"' + json.dumps(list(in_dict[f])) + '"'
                out_dict[f]=j
            else:
                j = json.dumps(in_dict[f])
                out_dict[f]=j

        fields = [v for k,v in sorted (out_dict.items(), key=operator.itemgetter(0) )]

        string = ",".join(fields)
        return string

    rdd = prediction.rdd.map(RowToStr)
    rdd.saveAsTextFile(output_path)

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






