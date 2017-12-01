#!/usr/bin/python3

import json
import operator
from collections import defaultdict
from collections import Counter
import numpy as np
import pandas as pd
import sklearn.cluster
import math

def create_model(input_file,out_model_file,num_points ):


    samples=defaultdict(lambda: defaultdict(Counter))
    weights=defaultdict(lambda: Counter () )
    model={}

    for line in open(input_file,"r"):
        page=json.loads(line)
        name = page["__name__"]
   

        for support_ext in page:
            if "__" in support_ext or page[support_ext] == -1:
                continue
            for support_int in page:
                if "__" in support_int or page[support_int] == -1:
                    continue
                if page[support_ext] > page[support_int]:
                    samples[name][support_ext][support_int] +=1
    '''        
    for name in samples:
        matrix=pd.DataFrame(samples[name])
        #weights[name] = dict(matrix.sum(axis=0))
        weights[name] = dict( matrix.sum(axis=0) / (matrix.sum(axis=1) + matrix.sum(axis=0)) )
    '''   
    for name in samples:
        matrix=pd.DataFrame(samples[name])
        weights[name] = dict(matrix.sum(axis=0))        
        
    '''     
    for name in samples:
        matrix=pd.DataFrame(samples[name])
        for row_name, row in matrix.iterrows():
            score=0
            for col_name, col in row.iteritems():
                if col_name!=row_name:
                    if matrix.ix[row_name][col_name] + matrix.ix[col_name][row_name] > 0:
                        score += matrix.ix[col_name][row_name] / ( matrix.ix[row_name][col_name] + matrix.ix[col_name][row_name] )
            weights[name][row_name] = score
    '''
        
    for page in weights:  
        current_model=[]

        current_set=set()
        sorted_supports=sorted(weights[page].items(), key=operator.itemgetter(1))     

        current_model=chunkIt( [s for s,n in sorted_supports] ,num_points)
        
        model[page]=current_model  
    
        
    json.dump(model, open(out_model_file, "w"))
    

def get_clusters_nb (n_points):
    if n_points == 1:
        return 1
    if n_points == 2 or n_points == 3:
        return 2
    else: 
        return int ( math.sqrt(n_points))

def chunkIt(seq, num):
    if len (seq)<num:
        return [[e] for e in seq] + [[]] * (num-len(seq))
    avg = len(seq) / float(num)
    out = []
    last = 0.0
    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg
    return out[:num]
  


           
