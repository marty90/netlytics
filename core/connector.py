from abc import ABCMeta, abstractmethod

# Implement the abstract algo
class Connector(object):
    __metaclass__ = ABCMeta

    def __init__(self, input_path, input_date_start,input_date_end):
        self.input_path = input_path
        self.input_date_start = input_date_start
        self.input_date_end = input_date_end

    def set_schema(self,schema):
        self.schema = schema

    # Class Information
    @property
    def name(self):
        return self._name
    @property
    def input_tool(self):
        return self._input_tool
    @property
    def output_type(self):
        return self._output_type

    # Run the algorithm
    @abstractmethod
    def get_DF(self, spark_context, sql_context): pass



