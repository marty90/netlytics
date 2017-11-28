from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.functions import col, when
from pyspark.sql import Row
from pyspark.sql.types import StructType
from pyspark.ml.linalg import DenseVector
from pyspark.ml.feature import Normalizer
from datetime import timedelta, date
import numpy as np
import operator
import json
import importlib
import os
import zipfile
import operator


# Create a data table, given connector and path
def get_dataset(sc,spark,base_path,connector,input_path,start_day, end_day ):
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

    # Return
    return dataset


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



def transform(df, spark, sql_query = None, numerical_features = [], categorical_features = [],\
              normalize = True, normalize_p=2):
    
    # Apply SQL query
    if sql_query != None:

        df.createOrReplaceTempView("netlytics")
        # Execute Query
        result_df = spark.sql(sql_query)
        df = result_df

    # Transform Strings in OneHot
    schema = df.schema
    feat_to_type = {}
    for struct in schema:
        feat_to_type[struct.name] = str(struct.dataType)
    
    for feature in categorical_features:

        # Replaces None
        k = col(feature)
        df = df.withColumn(feature, when(k.isNull(), "__NA__").otherwise(k))

        stringIndexer = StringIndexer(inputCol=feature, outputCol=feature + "_indexed",handleInvalid="skip")
        model = stringIndexer.fit(df)
        df = model.transform(df)

        encoder = OneHotEncoder(inputCol=feature + "_indexed", outputCol=feature + "_encoded")
        df = encoder.transform(df)
    

        
    # Extract Features
    def extract_features (row,numerical_features,feat_to_type):
        output_features = {}

        fields = list(row.asDict().keys())
        for field in fields:
            if field in numerical_features and feat_to_type[field] != "StringType":
                output_features[field] = float(row[field])
            if field.endswith("_encoded"):
                output_list = list(row[field])
                for i,v in enumerate(output_list):
                    tmp_field = field  + "_" + str(i)
                    output_features[tmp_field] = float(v)

        features = [v for k,v in sorted ( output_features.items(), key = operator.itemgetter(0)) ]

        old_dict = row.asDict()
        old_dict["features"] = DenseVector(features)
        new_row = Row(**old_dict)
        return new_row

    #spark = df.rdd.
    rdd = df.rdd.map(lambda row: extract_features(row, numerical_features,feat_to_type))
    df = spark.createDataFrame(rdd,samplingRatio=1,verifySchema=False)

    # Normalize
    if normalize:
        normalizer = Normalizer(inputCol="features", outputCol="featuresNorm", p=normalize_p)
        df = normalizer.transform(df)
        df = df.drop("features")
        df = df.withColumnRenamed("featuresNorm","features")

    # Delete intermediate columns:
    schema = df.schema
    feat_to_type = {}
    for struct in schema:
        feat_to_type[struct.name] = str(struct.dataType)

    for feature in feat_to_type:
        if feat_to_type[feature] != "StringType":
            if feature.endswith("_encoded") or feature.endswith("_indexed"):
                df = df.drop(feature) 

    return df        


