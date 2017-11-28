from __future__ import print_function
from core.anomaly_detection_algo import AnomalyDetectionAlgo
import core.S_H_ESD
import pandas as pd

# DNS Manipulations
class S_H_ESD(AnomalyDetectionAlgo):

    # Class Information
    _name = "BisectingKMeans"
    parameters={"period": None, "alpha":0.025, "hybrid": True }

    # Run the algorithm
    def run(self, input_dataframe):

        parameters=self.parameters
  
        df=input_dataframe.toPandas()
        
        time = list(df["time"])
        value = [ v[0] for v in list(df["features"]) ]

        seasons, trend, residual, outliers = core.S_H_ESD.S_H_ESD(value, \
                                                                  parameters["period"],
                                                                  alpha=parameters["alpha"],
                                                                  hybrid=parameters["hybrid"])

        
        booleans = [False] * len(value) 
        for index in outliers:
              booleans[index] = True

        output_list = list(zip( value, trend,residual,booleans ) )
        
        output_df = pd.DataFrame(output_list, index = time, columns= ["value","trend","residual","outlier"])
        output_df.index = pd.to_datetime(output_df.index,unit='s')


        return output_df



