#!/usr/bin/python3
from __future__ import print_function
import json
from collections import defaultdict
from collections import Counter
import numpy as np
import pandas as pd

def use_model(input_visits,input_model_file,out_log_file,n_checkpoints  ):

    
    occ_before=Counter()
    occ_after =Counter()
    out_log = open(out_log_file,"w")
    input_model = json.load(open(input_model_file,"r"))

    cp_string = " " .join( [ "cp_" + str(i+1) for i in range(n_checkpoints) ] )
    print ( "time c_ip page",  cp_string, file=out_log)
  

    for line in open(input_visits,"r"):
        visit=json.loads(line)

        
        page=visit["__name__"]

        time = visit["__time__"]
        c_ip = visit["__c_ip__"]
        if not page in input_model:
            continue

        model_to_fill= [ {d:-1 for d in node } for node in input_model[page]]

        for support in visit:
            if "__" in support or visit[support]==-1:
                continue
            for node in model_to_fill:
                if support in node:
                    node[support]=visit[support]

        node_time = []
        for node in model_to_fill:
            times = [t for t in node.values() if t!=-1]
            if times == []:
                cp_time = "-"
            else:
                #cp_time = statistics.mean(times)
                cp_time = max(times)
            node_time.append(str(cp_time))
        
        print ( time, c_ip, \
                page, \
                " ".join(node_time), file=out_log)

      
