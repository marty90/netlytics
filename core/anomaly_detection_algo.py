from abc import ABCMeta, abstractmethod


# Implement the abstract AnomalyDetection algo
class AnomalyDetectionAlgo:
    __metaclass__ = ABCMeta

    # Algo Parameter
    parameters = {}

    def __init__(self, parameters={} ):
        for p in parameters:
            self.parameters[p] = parameters[p]
        

    # Class Information
    @property
    def name(self):
        return self._name

    # Run the algorithm. Must return a pandas local datatframe
    @abstractmethod
    def run(self, input_dataframe): pass



