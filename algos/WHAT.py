from __future__ import print_function
from core.algo import Algo
import operator
import os
import re
from collections import Counter
from collections import defaultdict
from collections import deque
import json
import math
from collections import namedtuple

time_resoultion=24*3600*1000


# Define Window named tuple
Window = namedtuple( "wnd", ["start", "end"])

# DNS Manipulations
class WHAT(Algo):

    # Class Information
    _name = "WHAT"
    _input_type="NamedFlows"
    parameters={"OW":10000, "GAP":10000, "EW":5000, "MINFREQ":0.05, "CORES":[], "N_TOP":50}

    # Run the algorithm
    def run(self):

        this_DF = self.input_DF
        OW = self.parameters["OW"]
        GAP = self.parameters["GAP"]
        TIMEOUT = self.parameters["EW"]
        MINFREQ = self.parameters["MINFREQ"]
        CORES =   set(self.parameters["CORES"])

        N_TOP = self.parameters["N_TOP"]

        bags_and_occ = this_DF.rdd.map(mapLine).sortByKey()\
                          .mapPartitions(lambda t: getBags(t,CORES, GAP, OW)  )\
                          .reduceByKey(lambda s1,s2: reduceBags(s1,s2,CORES) ).collect()[0][1]


        occurrencies = bags_and_occ["occurrencies"]
        bags = bags_and_occ["bags"]
        
        for cd in bags:
            for d in bags[cd]:
                bags[cd][d] = float(bags[cd][d])/occurrencies[cd]
           
    
    
        json.dump(bags_and_occ, open(self.temp_dir_local + "/bags.json" , "w") )

        bags_filtered = filter_rules(bags_and_occ, MINFREQ)
        BAGS  = bags_filtered

        samples = this_DF.rdd.map(mapLineClassify).groupByKey().flatMap(lambda p : getFlows(p, BAGS,TIMEOUT))

        couples = samples.map(lambda t : (t[11], t[7] + t[9])).reduceByKey(lambda b1,b2: b1+b2)

        this_top = couples.top(N_TOP, key=lambda t : t[1] )

        directory = self.output_dir

        if not os.path.exists(directory):
            os.makedirs(directory)

        fo = open(directory + "/report.csv","w")
        print ("service", "bytes", sep=",", file=fo)
        for d,b in this_top:
            print (d,b, sep=",", file=fo)

        fo.close()


def filter_rules(rules_json, ratio_th):
    rules=rules_json["bags"]
    all_domains=Counter()
    rules_new = defaultdict(dict)
    for core in rules:
        for found in rules[core]:
            if rules[core][found]>= ratio_th:
                rules_new[core][found] = rules[core][found]
                all_domains[found] += 1
                
    length = len (rules_new)            
    for core in rules_new:
        for found in rules_new[core]:
            rules_new[core][found] =  rules_new[core][found] * math.log10(length/all_domains[found])          

                
    return rules_new


def mapLine(line):
    
    time_start = line.time_start / 1000
    time_end = line.time_end  / 1000
    c_ip = line.c_ip
    c_port = line.c_port
    s_ip = line.s_ip
    s_port = line.s_port
    c_pkt = line.c_pkt
    c_bytes = line.c_bytes
    s_pkt = line.s_pkt
    s_bytes = line.s_bytes
    name = line.name

    tup=(time_start, time_end, c_ip, c_port, s_ip, s_port, c_pkt, c_bytes, s_pkt, s_bytes, name)
  
    return (time_start, tup)
    
def getBags(iterator, CORES, GAP, OW):


    bags={}
    occurrencies={}

    windows={}
    times_activity={}

    # Crete sorted array
    rows=[]
    for time, row in iterator:
        time, time_end, c_ip, c_port, s_ip, s_port, c_pkt, c_bytes, s_pkt, s_bytes, name = row

        # Update last activity time for that client always
        if c_ip in times_activity:
            old_time=times_activity[c_ip]
        else:
            old_time=-1
            
        times_activity[c_ip]=time
        
        for client in list(windows.keys()):
            if time - windows[client]["time"] >OW:
                del windows[client]
                
        # Check last activity time of that client
        if name in CORES and  old_time != -1 and time - old_time > GAP:
            windows[c_ip]={"name":name,"time":time}      
            if not name in occurrencies:
                occurrencies[name] = 0
            occurrencies[name] +=1
        elif c_ip in windows:
            core = windows[c_ip]["name"]
            if not core in bags:
                bags[core] = {}
            filtered_name = filter_name(name)
            if not filtered_name in bags[core]:
                bags[core][filtered_name]=0
            bags[core][filtered_name] +=1

    return [ (1, { "bags": bags, "occurrencies": occurrencies }) ]

def reduceBags( dictionary1, dictionary2, CORES ):

    merged_dictionary = { "bags": {}, "occurrencies": {} }

    bags1 = dictionary1["bags"]
    bags2 = dictionary2["bags"]

    occ1 = dictionary1["occurrencies"]
    occ2 = dictionary2["occurrencies"]

    for core in CORES:
        if core in bags1:
            counter1 = Counter(bags1[core])
        else:
            counter1 = Counter()
        if core in bags2:
            counter2 = Counter(bags2[core])
        else:
            counter2 = Counter()
        merged_bag = dict(counter1 + counter2)
        merged_dictionary["bags"][core] = merged_bag

    merged_occurrencies = dict( Counter(occ1) + Counter(occ2) )
    merged_dictionary["occurrencies"] = merged_occurrencies

    return merged_dictionary

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



def mapLineClassify(line):
    global time_resoultion
    

    time_start = line.time_start  / 1000
    time_end = line.time_end  / 1000
    c_ip = line.c_ip
    c_port = line.c_port
    s_ip = line.s_ip
    s_port = line.s_port
    c_pkt = line.c_pkt
    c_bytes = line.c_bytes
    s_pkt = line.s_pkt
    s_bytes = line.s_bytes
    name = line.name

    time_bin = int(time_start/time_resoultion)
    tup=(time_start, time_end, c_ip, c_port, s_ip, s_port, c_pkt, c_bytes, s_pkt, s_bytes, name)

    return ( c_ip + " " + str(time_bin), tup)

def getFlows(tup,BAGS,TIMEOUT):

    rules = BAGS

    # Crete sorted array
    last_cores = defaultdict(dict)
    last_flows = defaultdict(deque)
    out_flows = []
    i = 0
    
    rows = sorted( tup[1], key = lambda line: line[0] )
    
    for row in rows:
        

        time, time_end, c_ip, c_port, s_ip, s_port, c_pkt, c_bytes, s_pkt, s_bytes, name = row
        length = time_end-time
        bytes = c_bytes + s_bytes
        name_filtered = filter_name(name)

        # Delete old core domains - "Garbage collection"
        for core in list(last_cores[c_ip].keys()):
            if last_cores[c_ip][core].end < time:
                del last_cores[c_ip][core] 

        # Delete list of last domains
        for old_time, old_domain in list(last_flows[c_ip]):
            if time - old_time > TIMEOUT*5:
                last_flows[c_ip].popleft()

        # Check unnamed flows.
        if name=="-":
            prevision = "UNKNOWN"
            out_row = row + (prevision,)
            out_flows.append(out_row)

        # Check if it is a core                
        elif name in rules and is_valid_trigger(c_ip, last_flows[c_ip], last_cores[c_ip], name, time, rows, i, rules, TIMEOUT):

            last_cores[c_ip][name] = Window (time, time + length + TIMEOUT )

            # Write log
            prevision = name
            out_row = row + (prevision,)
            out_flows.append(out_row)

            # Add history flows
            tup = (time, name)
            last_flows[c_ip].append(tup)
        
        # It is not a trigger     
        else:
            # Check last core domains
            if c_ip in last_cores:
                last_cores_sorted = sorted(last_cores[c_ip].items(), key = lambda t: t[1].start, reverse=True)
                max_score = 0
                for core, visit_time  in last_cores_sorted:
                    if name_filtered in rules[core]:
                        max_trigger=core
                        max_score=1
                        last_cores[c_ip][core]=Window(last_cores[c_ip][core].start, max ( last_cores[c_ip][core].end, time + length + TIMEOUT) ) 
                        #score = (math.pow(10, - (time - visit_time)/timeout  ) - 0.1 ) * ( rules[core][name_filtered])
                        #if score > max_score:
                        #    max_score = score
                        #    max_trigger=core
                        break
            
            # Write log
            if max_score == 0:
                prevision = "UNKNOWN"
                out_row = row + (prevision,)
                out_flows.append(out_row)
            else:
                prevision = max_trigger
                out_row = row + (prevision,)
                out_flows.append(out_row)
            
            # Add history flows
            tup = (time, name_filtered)
            last_flows[c_ip].append(tup)

        i+=1

    return out_flows




def is_valid_trigger (c_ip, last_flows_deque, last_cores_dict, name, time, rows, i, rules, TIMEOUT):

    # Return True if there isn't any open window
    last_cores_sorted = sorted (last_cores_dict.items(), key = lambda t: t[1].start, reverse=True )
    if last_cores_sorted == []:
        return True
    
    # Find the last window. Search for an open window whose bag contains the current domain
    found_core_in_bag = False
    for last_core, last_window_times in last_cores_sorted:
        if name in rules[last_core]:
            found_core_in_bag = True
            break
    if found_core_in_bag == False:
        return True

        
    found_last_core = False
    last_window=Counter()
    for last_flow_time, domain in last_flows_deque:
        if last_flow_time == last_window_times.start:
            found_last_core=True
            continue
        if found_last_core:
            last_window[domain]+=1
    last_window[name]+=1  
    # If last window was older than flow cache, consider the trigger valid
    if not found_last_core:
        return True
          
    # Find current window
    current_window=Counter()
    index = i+1
    while True:
        if index >= len(rows):
            break   
        #(row_ip, row_length, row_time, bytes, row_name) = parse_row(rows[index])
        row_time, _ , row_ip,  _ ,  _ , _ , _ , _ , _ , _ , row_name = rows[index]
        index +=1
        if row_time - time > TIMEOUT:
            break
        if row_ip != c_ip:
            continue
        if row_name=="-":
            continue
        #if row_name in rules:
        #    break
        current_window[row_name]+=1

        
    last_window=last_window + current_window
    #print(last_core, last_window)
    #print("\t", name, current_window)

    last_score = distance_window_bag(last_window, rules[last_core])
    current_score = distance_window_bag(current_window, rules[name])
    
    if current_score >= last_score:
        return True
    else:
        return False
    

def distance_window_bag ( window, bag ):

    if len(window)==0:
        return 0
    score = 0
    for domain in window:
        if domain in bag:
            tmp_score = window[domain]*bag[domain]
        else:
            tmp_score = 0
        score+=tmp_score
    score = score /len(window)
    return score

def import_dict(file):
    json_data=open(file,"r").read()
    return json.loads(json_data)

def filter_rules(rules_json, ratio_th):
    rules=rules_json["bags"]
    all_domains=Counter()
    rules_new = defaultdict(dict)
    for core in rules:
        for found in rules[core]:
            if rules[core][found]>= ratio_th:
                rules_new[core][found] = rules[core][found]
                all_domains[found] += 1
                
    length = len (rules_new)            
    for core in rules_new:
        for found in rules_new[core]:
            rules_new[core][found] =  rules_new[core][found] * math.log10(length/all_domains[found])          

                
    return rules_new





