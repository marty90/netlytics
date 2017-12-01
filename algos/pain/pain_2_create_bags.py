#!/usr/bin/python3
from __future__ import division
import json
import re
from collections import defaultdict
from collections import Counter
import numpy as np


def create_bags(input_json, output_json, min_freq ):

    min_times=0
    bags_aggregated=defaultdict(lambda: defaultdict(lambda: [] ))
    bags_consolidated=defaultdict(lambda: defaultdict(dict))
    occurrencies=Counter() 
 
    for line in open(input_json, "r"):
        window=json.loads(line)
        core_domain=window["__name__"]
        for support_domain in window:
            if support_domain == "__name__":
                continue
            bags_aggregated[core_domain][support_domain].append(window[support_domain])
        occurrencies[core_domain]+=1


    for domain in bags_aggregated:

        if occurrencies[domain] < min_times:
                continue
        for support_domain in bags_aggregated[domain]:

            freq = len(bags_aggregated[domain][support_domain]) / occurrencies[domain] *100
            if len(bags_aggregated[domain][support_domain]) < 2 or freq < min_freq:   
                continue
            bags_consolidated[domain][support_domain]["freq"]= freq
            bags_consolidated[domain][support_domain]["avg_time"] = np.mean( bags_aggregated[domain][support_domain] )
            bags_consolidated[domain][support_domain]["stdev_time"] = np.std( bags_aggregated[domain][support_domain] )
    
        
    json.dump(bags_consolidated, open(output_json, "w"))


