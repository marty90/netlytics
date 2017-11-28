from __future__ import print_function
from core.anomaly_detection_algo import AnomalyDetectionAlgo
import core.S_H_ESD

# DNS Manipulations
class S_H_ESD(AnomalyDetectionAlgo):

    # Class Information
    _name = "BisectingKMeans"
    parameters={"period": None, "alpha":0.025, "hybrid": True )

    # Run the algorithm
    def run(self, input_dataframe):

        df=input_dataframe.toPandas()
        






