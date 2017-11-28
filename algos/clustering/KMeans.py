from __future__ import print_function
from core.clustering_algo import ClusteringAlgo
from pyspark.ml.clustering import KMeans
import operator
import os

# DNS Manipulations
class KMeans(ClusteringAlgo):

    # Class Information
    _name = "KMeans"
    parameters={"K":5,"seed":1}

    # Run the algorithm
    def run(self, input_dataframe):

        K = self.parameters["K"]
        seed = self.parameters["seed"]
        kmeans = KMeans().setK(K).setSeed(seed)

        model = kmeans.fit(input_dataframe)        
        prediction = model.transform(input_dataframe)
        return prediction





