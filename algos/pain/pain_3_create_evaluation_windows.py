#!/usr/bin/python3
import operator
import json

import re
from collections import Counter
from collections import defaultdict
from collections import deque
from subprocess import call
from collections import namedtuple

# Define Window named tuple
Window = namedtuple( "wnd", ["start", "end"])


def mapLine(line):

    time=line.time_start
    
    return ( time , line)
    
def getBags(iterator, rules, window_len):

    bags=[]

    windows={}
    current_bag={}
    times_activity={}

    # Crete sorted array
    rows=[]
    first=True
    for time, row in iterator:
        
        c_ip=row.c_ip
        s_ip=row.s_ip
        s_port = row.s_port
        time= row.time_start
        name = row.name

        # Update last activity time for that client always
        if c_ip in times_activity:
            old_time=times_activity[c_ip]
        else:
            old_time=-1
            
        times_activity[c_ip]=time    
        
        filtered_name = filter_name(name)
        # Garbage collection
        for client in list(windows.keys()):
            if time - windows[client]["time"] > window_len:
                del windows[client]
                bags.append(json.dumps(current_bag[client]))
                
        # Open a new window
        if name in rules:
            windows[c_ip]={"name":name,"time":time}      
            current_bag[c_ip] = {"__name__": name, "__c_ip__": c_ip, "__time__": time}
            for support in rules[name]:
                current_bag[c_ip][support] = -1
            
        # Add support domains
        elif c_ip in windows and filtered_name in rules[windows[c_ip]["name"]]:
            core = windows[c_ip]["name"]
            core_time = windows[c_ip]["time"]
            # Consider support domains: (i) not already seen, (ii) carrying HTTP or HTTPS, (iii) having first data byte before timeout
            if current_bag[c_ip][filtered_name] == -1  and (time - core_time) < window_len: 
                current_bag[c_ip][filtered_name] = (time - core_time)  
            
    return bags

    

def filter_name (name):
    
    # Strip cloudfront.net
    filtered_name = re.sub('[a-z0-9]+\.cloudfront.net', "X.cloudfront.net", name)
    filtered_name = re.sub('[a-z0-9]+\.profile\..*\.cloudfront.net', "X.cloudfront.net", filtered_name)
      
    # Strip googlevideo.com
    filtered_name = re.sub('---sn-.*\.googlevideo\.com',"---sn-X.googlevideo.com", filtered_name)
    filtered_name = re.sub('---sn-.*\.c\.pack\.google\.com',"---sn-X.c.pack.google.com", filtered_name)
    
    # Strip digits
    filtered_name = re.sub('\d+',"D", filtered_name)
    
    # Strip "C"
    filtered_name = re.sub('((?<=[-\._D])|^)[a-z](?=[-\._D])',"C", filtered_name)
    
    return filtered_name



