#!/usr/bin/python3
import operator
import json
import time
import sys
import re
from collections import Counter

    
def mapLine(line):
    time=line.time_start
    
    return ( time , line)
    
def getBags(iterator, core_domains, window_len, gap):

    bags=[]

    windows={}
    times_activity={}
    current_bag={}

    # Crete sorted array
    rows=[]
    first=True
    for time, row in iterator:
        
        # Get name. Precedence to SNI and HN

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
        

        # Garbage collection
        for client in list(windows.keys()):
            if time - windows[client]["time"] > window_len:
                del windows[client]
                bags.append(json.dumps(current_bag[client]))
                
        # Open a new window
        if name in core_domains and  old_time != -1 and time - old_time > gap:
            windows[c_ip]={"name":name,"time":time}      
            current_bag[c_ip] = {"__name__": name}
            
        # Add support domains
        elif c_ip in windows:
            core = windows[c_ip]["name"]
            core_time = windows[c_ip]["time"]
            filtered_name = filter_name(name)
            if not filtered_name in current_bag[c_ip]:
                current_bag[c_ip][filtered_name] = time - core_time 
            
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

