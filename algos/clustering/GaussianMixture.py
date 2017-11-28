from __future__ import print_function
from core.clustering_algo import ClusteringAlgo
import pyspark.ml.clustering
import operator
import os

# DNS Manipulations
class BisectingKMeans(ClusteringAlgo):

    # Class Information
    _name = "BisectingKMeans"
    parameters={"K":5,"seed":1}

    # Run the algorithm
    def run(self, input_dataframe):

        K = self.parameters["K"]
        seed = self.parameters["seed"]
        kmeans = pyspark.ml.clustering.BisectingKMeans().setK(K).setSeed(seed)

        model = kmeans.fit(input_dataframe)        
        prediction = model.transform(input_dataframe)
        return prediction





