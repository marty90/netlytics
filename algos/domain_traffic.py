from __future__ import print_function
from core.algo import Algo
import operator
import os

# DNS Manipulations
class DomainTraffic(Algo):

    # Class Information
    _name = "domain_traffic"
    _input_type="NamedFlows"
    parameters={"N":50}

    # Run the algorithm
    def run(self):
        this_DF = self.input_DF
        N_TOP = self.parameters["N"]

        this_RDD_mapped = this_DF.rdd.map(mapRDD)
        this_RDD_traffic = this_RDD_mapped.reduceByKey(lambda b1,b2:b1+b2)

        this_top = this_RDD_traffic.top(N_TOP, key=lambda t : t[1] )

        directory = self.output_dir

        if not os.path.exists(directory):
            os.makedirs(directory)

        fo = open(directory + "/report.csv","w")
        print ("domain", "bytes", sep=",", file=fo)
        for d,b in this_top:
            print (d,b, sep=",", file=fo)

        fo.close()


def mapRDD(line):

    name = line.name
    s_bytes= line.s_bytes
    c_bytes= line.c_bytes

    return (name, int(s_bytes) + int(c_bytes))


