from __future__ import print_function
from core.anomaly_detection_algo import AnomalyDetectionAlgo
import core.S_H_ESD
from pyspark.sql import Row
import pandas as pd

# DNS Manipulations
class UnivariateAnomalies(AnomalyDetectionAlgo):

    # Class Information
    _name = "UnivariateAnomalies"
    parameters={"method": "gaussian"}

    # Run the algorithm
    def run(self, input_dataframe):

        parameters=self.parameters
  
        if parameters["method"]=="gaussian":
            input_dataframe=input_dataframe.rdd.map(extract_features).toDF(sampleRatio=1)
            values_rdd = input_dataframe.rdd.map(lambda row: row["features"])
            mean = values_rdd.mean()
            stdev = values_rdd.stdev()

            upper_bound = mean + 3*stdev
            lower_bound = mean - 3*stdev

            anomalies = input_dataframe.filter( \
                          (input_dataframe.features < lower_bound) |  (input_dataframe.features > upper_bound) )



        elif parameters["method"]=="boxplot":
            input_dataframe=input_dataframe.rdd.map(extract_features).toDF(sampleRatio=1)

            q1, q3 = input_dataframe.approxQuantile("features", [0.25,0.75], 0 )

            upper_bound = q3 + 3*(q3-q1)
            lower_bound = q1 - 3*(q3-q1)

            anomalies = input_dataframe.filter( \
                           (input_dataframe.features < lower_bound) |  (input_dataframe.features > upper_bound) )
            

        return anomalies.toPandas()


def extract_features (row):

    fields = row.asDict()
    fields["features"]=float(fields["features"][0])
    new_row = Row (**fields)

    return new_row







