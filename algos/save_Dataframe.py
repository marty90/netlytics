from __future__ import print_function
from core.algo import Algo
import operator
import os

# DNS Manipulations
class SaveDataFrame(Algo):

    # Class Information
    _name = "SaveDataFrame"
    _input_type="*"
    parameters={"N":-1}

    # Run the algorithm
    def run(self):
        this_DF = self.input_DF

        if self.parameters["N"]!=-1:
            df_pandas = this_DF.limit(self.parameters["N"]).toPandas()
        else:
            df_pandas = this_DF.toPandas()

        df_pandas.to_csv(self.output_dir + "/dataframe.csv", encoding="raw_unicode_escape")



def mapRDD(line):
    s_ip=line.s_ip

    return (s_ip, 1)





