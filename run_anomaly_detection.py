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
from pyspark.ml.linalg import DenseVector
import operator
import core.utils


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

    # Create the dataframe
    dataset = core.utils.get_dataset(sc,spark,base_path,connector,input_path,start_day, end_day )
    
    # Pre process the dataframe
    manipulated_dataset = core.utils.transform(dataset,spark,\
                                                sql_query = query,\
                                                numerical_features = numerical_features,
                                                categorical_features = categorical_features,
                                                normalize=normalize)
    # Run Anomaly Detection
    AD_algo_module = my_import(algo,sc)
    AD_algo_instance = AD_algo_module( json.loads(params) )
    pandas_df = AD_algo_instance.run(manipulated_dataset)

    # Save Output in CSV
    # Create output_path if not existing
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    pandas_df.to_csv(output_path + "/results.csv")


def my_import(name,sc):
    labels = name.split(".")
    base_name = ".".join(labels[:-1])
    module = importlib.import_module(base_name)
    my_class = getattr(module, labels[-1])
    return my_class

main()






