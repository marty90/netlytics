from __future__ import print_function
from core.algo import Algo

# Import code
import algos.pain.pain_1_create_observation_windows
import algos.pain.pain_2_create_bags
import algos.pain.pain_3_create_evaluation_windows
import algos.pain.pain_4_create_model_matrix
import algos.pain.pain_5_use_model
from shutil import copyfile
import numpy as np 
from collections import Counter
import json
from subprocess import call

hdfs_command="hdfs"

# DNS Manipulations
class Pain(Algo):

    # Class Information
    _name = "Pain"
    _input_type="NamedFlows"
    parameters={"EW":30, "GAP": 5, "MINFREQ": 0.25, "N_CP":4, \
                "CORES": ["www.repubblica.it","www.lastampa.it","www.ilmeteo.it"]}

    # Run the algorithm
    def run(self):

        delta_t = self.parameters["EW"]
        gap = self.parameters["GAP"]
        min_freq = self.parameters["MINFREQ"]
        n_cp = self.parameters["N_CP"]
        cores = self.parameters["CORES"]

        temp_dir_HDFS = self.temp_dir_HDFS
        temp_dir_local = self.temp_dir_local

        input_DF = self.input_DF             

        # 1. Create Observation Windows
        f=algos.pain.pain_1_create_observation_windows.getBags
        def get_bags(iterator):
            return f(iterator, cores, delta_t, gap)

        OWs = input_DF.rdd\
              .map(algos.pain.pain_1_create_observation_windows.mapLine)\
              .sortByKey()\
              .mapPartitions(get_bags)


        call (hdfs_command + " dfs -rm -r " + temp_dir_HDFS + "/OWs.json", shell=True)
        OWs.saveAsTextFile(temp_dir_HDFS + "/OWs.json")
        call (hdfs_command + " dfs -getmerge " + \
                            temp_dir_HDFS  + "/OWs.json" + " " + \
                            temp_dir_local + "/OWs.json" , shell=True) 

        # 2. Create Bags
        algos.pain.pain_2_create_bags.create_bags(temp_dir_local + "/OWs.json", \
                                                  temp_dir_local + "/bags.json", \
                                                  min_freq*100 )

        # 3. Create EWs
        bags = json.load(open(temp_dir_local + "/bags.json","r"))
        f = algos.pain.pain_3_create_evaluation_windows.getBags
        def get_bags(iterator):
            return f(iterator, bags, delta_t)
        EWs = input_DF.rdd\
                  .map(algos.pain.pain_3_create_evaluation_windows.mapLine)\
                  .sortByKey()\
                  .mapPartitions(get_bags)

        call (hdfs_command + " dfs -rm -r " + temp_dir_HDFS + "/EWs.json", shell=True)
        EWs.saveAsTextFile(temp_dir_HDFS + "/EWs.json")
        call (hdfs_command + " dfs -getmerge " + \
                            temp_dir_HDFS  + "/EWs.json" + " " + \
                            temp_dir_local + "/EWs.json" , shell=True) 

        # 4. Create Model
        algos.pain.pain_4_create_model_matrix.create_model(temp_dir_local + "/EWs.json", \
                                                  temp_dir_local + "/model.json",
                                                  n_cp)


        # 5. Use Model
        algos.pain.pain_5_use_model.use_model(temp_dir_local + "/EWs.json",\
                                              temp_dir_local + "/model.json",\
                                              self.output_dir + "/final_report.csv",\
                                              n_cp)

        copyfile(temp_dir_local + "/model.json", self.output_dir + "/model.json")










