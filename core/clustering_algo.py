from abc import ABCMeta, abstractmethod

# Implement the abstract clustering algo
class ClusteringAlgo:
    __metaclass__ = ABCMeta

    # Algo Parameter
    parameters = {}

    def __init__(self, parameters={} ):
        for p in parameters
            self.parameters[p] = parameters[p]
        

    # Class Information
    @property
    def name(self):
        return self._name

    # Run the algorithm
    @abstractmethod
    def run(self, input_dataframe): pass



