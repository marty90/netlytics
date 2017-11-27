from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.functions import col, when
import numpy as np
from pyspark.ml.feature import Normalizer
import operator
from pyspark.sql import Row
from pyspark.ml.linalg import DenseVector

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


