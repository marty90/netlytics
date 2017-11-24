from abc import ABCMeta, abstractmethod

# Implement the abstract algo
class Algo:
    __metaclass__ = ABCMeta

    # Algo Parameter
    parameters = {}

    def __init__(self, input_DF, output_dir, \
                 temp_dir_local=None,  temp_dir_HDFS=None, \
                 persistent_dir_local=None, persistent_dir_HDFS=None ):
        self.input_DF = input_DF
        self.output_dir = output_dir
        self.temp_dir_local = temp_dir_local
        self.temp_dir_HDFS = temp_dir_HDFS
        self.persistent_dir_local = persistent_dir_local
        self.persistent_dir_HDFS = persistent_dir_HDFS

    # Class Information
    @property
    def name(self):
        return self._name
    @property
    def input_type(self):
        return self._input_type

    # Run the algorithm
    @abstractmethod
    def run(self): pass



