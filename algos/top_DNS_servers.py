from __future__ import print_function
from core.algo import Algo
import operator
import os

# DNS Manipulations
class TopDNSServers(Algo):

    # Class Information
    _name = "TopDNSServers"
    _input_type="DNS"
    parameters={"N":50}

    # Run the algorithm
    def run(self):
        this_DF = self.input_DF
        N_TOP = self.parameters["N"]

        this_RDD_mapped = this_DF.rdd.map(mapRDD)
        this_RDD_N = this_RDD_mapped.reduceByKey(lambda b1,b2:b1+b2)

        this_top = this_RDD_N.top(N_TOP, key=lambda t : t[1] )

        directory = self.output_dir

        if not os.path.exists(directory):
            os.makedirs(directory)

        fo = open(directory + "/report.csv","w")
        print ("resolver", "queries", sep=",", file=fo)
        for d,b in this_top:
            print (d,b, sep=",", file=fo)

        fo.close()


def mapRDD(line):
    s_ip=line.s_ip

    return (s_ip, 1)





