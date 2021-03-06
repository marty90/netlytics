NetLytics
=========
NetLytics is a Hadoop-powered framework for performing advanced analytics on various kinds of networks logs.

It is able to parse log files generated by popular network softwares implementing HTTP proxy, DNS server and passive sniffers; 
it can also parse raw PCAP files.

It assumes that log files are stored on HDFS in a Hadoop based cluster.

NetLytics uses log files to perform a wide range of advanced network analytics for traffic monitoring and security purposes. All code is written in Python and uses Apache Spark.

For information about this Readme file and this tool please write to
[martino.trevisan@polito.it](mailto:martino.trevisan@polito.it)

# Table of Content
<!---
Done with https://github.com/ekalinin/github-markdown-toc )
-->

   * [NetLytics](#netlytics)
   * [Table of Content](#table-of-content)
   * [1. Prerequisites](#1-prerequisites)
   * [2. Architecture](#2-architecture)
      * [2.1 Tools generating Network Logs](#21-tools-generating-network-logs)
         * [Bro](#bro)
         * [Squid](#squid)
         * [Tstat](#tstat)
         * [Bind](#bind)
         * [Pcap Files](#pcap-files)
      * [2.2 Data storage](#22-data-storage)
      * [2.3 Connectors](#23-connectors)
      * [2.4 Data Tables](#24-data-tables)
   * [3. Algorithms](#3-algorithms)
      * [3.1 Clustering](#31-clustering)
      * [3.2 Anomaly Detection](#32-anomaly-detection)
      * [3.3 Advanced Analytics](#33-advanced-analytics)
      * [3.4 Running SQL queries](#34-running-sql-queries)
   * [4. Use NetLytics as a library](#4-use-netlytics-as-a-library)
      * [4.1 Get Data Table from raw logs](#41-get-data-table-from-raw-logs)
      * [4.2 Process a Data Table to extract features](#42-process-a-data-table-to-extract-features)
      * [4.3 Use algorithms to process Data Tables](#43-use-algorithms-to-process-data-tables)
         * [4.3.1 Clustering](#431-clustering)
         * [4.3.2 Anomaly Detection](#432-anomaly-detection)
         * [4.3.3 Advanced Analytics](#433-advanced-analytics)


# 1. Prerequisites
Netlytics is designed to work in the Hadoop ecosystem. As such, it needs HDFS and Apache Spark (>= 2.0.0) to respectively store and process log files. It uses python 2.7 and few python packages: you can install them with pip
```
sudo pip install zipfile scapy numpy pandas pyasn networkx seasonal requests six
```
If you are using a cluster, these packages must be installed on each worker node.

You can clone the GitHub repository with the command:
```
git clone https://github.com/marty90/netlytics
```

# 2. Architecture
NetLytics is composed on many building blocks illustrated in the figure below.
![alt text](https://github.com/marty90/netlytics/raw/master/images/netlytics_arch.png)

## 2.1 Tools generating Network Logs
NetLytics handles log files generated by a wide number of softwares.
From raw log files, NetLytics can create Data Tables for **DNS**, **HTTP** and **Named Flows**.
Not all tables can be created from all tools.

|**Tool** | **DNS**  |  **HTTP**   |   **Named Flows** |
|-------|-----|------|-------------|
|  **Bro**   | **Y**   | **Y**    | **Y**          |
|  **Squid** | N   | **Y**    | **Y**           |
|  **Tstat**  | **Y**   | **Y**    | **Y**           |
| **Bind** | **Y**   | N    | N           |
|  **PCAP** | **Y**   | N    | N           |

It not differently specified, NetLytics assumes that the folder structure and the file names are the default ones.
Currently Netlytics can parse log files generated by 5 tools.
### Bro
[Bro](https://www.bro.org/) is a network security monitor that passively analyzes traffic and produces several log files.
In particular, log files for TCP, HTTP and DNS traffic are generated.
Bro typically rotate log files every hour, and puts all log files generated in one day in a separate directory. The typical directory tree is the following:
```
2017-11-20/
\------------ dns.22.00.00-23.00.00.log.gz
\------------ dns.23.00.00-00.00.00.log.gz
\------------ http.13.00.00-14.00.00.log.gz
\------------ http.14.00.00-15.00.00.log.gz
\------------ ...
2017-11-21/
\------------ ...
```
NetLyitics assumes that this directory structure is replicated in HDFS.

NetLytics Data Tables: **DNS**, **HTTP** and **Named Flows**.

### Squid
[Squid](http://www.squid-cache.org/) is the most popular software HTTP proxy. It generates a log file where all HTTP transactions are recorded. It is typically stored in `/var/log/squid/access.log`; it can assume various formats, but NetLytics assumes the default one is used (called `squid format` in Squid documentation).

Squid does not handle log file rotation and storage, but users typically employ the [logrotate](https://linux.die.net/man/8/logrotate) utility to handle this.
`logrotate` periodically rotates log files and stores old ones with the name `access.log-YYYYMMDD` where `YYYY`, `MM` and `DD` are the current year, month and day respectively.
NetLytics assumes this name format is used to store log files on HDFS.

NetLytics Data Tables: **HTTP** and **Named Flows**.


### Tstat
[Tstat](http://tstat.polito.it/) is a network meter that passively analyzes traffic and produces rich log files.
In particular, log files for TCP, HTTP and DNS traffic are generated.

Tstat typically rotate log files every hour, and puts all log files generated in one day in a separate directory. The typical directory tree is the following:
```
2016
\------------ 04-Apr
              \------------ 2017_04_01_00_30.out
              \------------ 2017_04_01_01_30.out
              \------------ 2017_04_01_02_30.out
              \------------ ...
\------------ 05-May
              \------------ 2017_05_01_00_30.out
              \------------ 2017_05_01_01_30.out
              \------------ ...
\------------ ...
2017
\------------ 01-Jan
...
```
NetLyitics assumes that this directory structure is replicated in HDFS.

NetLytics Data Tables: **DNS**, **HTTP** and **Named Flows**.


### Bind
[Bind](https://www.isc.org/downloads/bind/) is the most popular software DNS server.
It can be configured to create log files changing the `/etc/bind/named.conf` files, adding:
```
logging {
  channel bind_log {
    file "/var/log/bind/bind.log" versions 3 size 5m;
    severity info;
    print-category yes;
    print-severity yes;
    print-time yes;
  };
  category queries { bind_log; };
};

```
The log file is created in `/var/log/bind/bind.log`.
Bind does not handle log file rotation and storage, but users typically employ the [logrotate](https://linux.die.net/man/8/logrotate) utility to handle this.
`logrotate` periodically rotates log files and stores old ones with the name `bind.log-YYYYMMDD` where `YYYY`, `MM` and `DD` are the current year, month and day respectively.
NetLytics assumes this name format is used to store log files on HDFS.

NetLytics Data Tables: **DNS**.

### Pcap Files
NetLytics can parse raw Pcap files to extract DNS data.
We reccommend to use the utility [dnscap](https://github.com/DNS-OARC/dnscap) to generate such Pcap files.
It can rotate log files after a configurable period of time.
Default Pcap file names have the format: `<base_name>.YYYYMMDD.HHmmSS.uSec`.
NetLytics assumes this format is used in HDFS.

NetLytics Data Tables: **DNS**.

## 2.2 Data storage
NetLytics assumes that network log files are stored on HDFS and accessible by the current Spark user.
Each tool producing

## 2.3 Connectors
The connectors are the software modules to parse log files and create Data Tables.
NetLytics parses log files on the original tool format, and creates on-the-fly Data Tables to be used by algorithms.

Each connector is identified by a *class name*, and is suited for generating a given *Data Table* from log file of a *tool*.

The following table illustrates the available connectors:

| **Tool**|**Data Table**| **Class Name**                                  |
|-------|------------|------------------------------------------------------|
| Tstat | DNS        | `connectors.tstat_to_DNS.Tstat_To_DNS`                 |
| Tstat | NamedFlows | `connectors.tstat_to_named_flows.Tstat_To_Named_Flows` |
| Tstat | HTTP       | `connectors.tstat_to_HTTP.Tstat_To_HTTP`               |
| Bro   | DNS        | `connectors.bro_to_DNS.Bro_To_DNS`                     |
| Bro   | HTTP       | `connectors.bro_to_HTTP.Bro_To_HTTP`                   |
| Bro   | NamedFlows | `connectors.bro_to_named_flows.Bro_To_Named_Flows`     |
| Squid | HTTP       | `connectors.squid_to_HTTP.Squid_To_HTTP`               |
| Squid | NamedFlows | `connectors.squid_to_named_flows.Squid_To_Named_Flows` |
| Bind  | DNS        | `connectors.bind_to_DNS.Bind_To_DNS`                   |
| PCAP  | DNS        | `connectors.PCAP_to_DNS.PCAP_To_DNS`                   |


## 2.4 Data Tables
A Data Table represents a dataset of network measurements under a certain period of time. It is implemented using Spark Dataframes.
The schemas of the available Data Tables are available under the directory `schema`.
Three types of Data Tables are handled by NetLytics:

* **DNS**: contains information about DNS traffic, such as queried domains, contacted resolvers, etc. It is generated by passive sniffers and DNS servers.

* **HTTP**: contains information HTTP transactions. It reports queried URLs, contacted servers, etc. It is generated by passive sniffers and HTTP servers. Limited information is available when encryption (SSL, TLS, HTTPS) is used.

* **Named Flows**: contains flow level measurements enriched with hostname of the server being contacted. Beside typical flow level statistics (number of packets and bytes), there is an explicit indication of the hostname of the server. This indication is typically generated using DPI on HTTP/TLS fields or with DNS traffic analysis. These logs are generated by passive sniffers and can be derived from HTTP proxy logs.

# 3. Algorithms
Using Data Tables, NetLyitics can run algorithms on data. Several algorithms are available, and users are encouraged to share their own to enrich this software.
Algorithms are divided in three categories: clustering, anomaly detection, and advanced analytics.
To run an algorithm, you must use the provided script, and specify an input dataset.

In the follwing, for exemplification, we suppose to have **Squid** log files available in a directory called `logs/squid`.

## 3.1 Clustering
Clustering algorithms group data toghether.

You must provide the algorithm, an input Data Table, and its parameters.
You can specify an SQL query used to preprocess the Data Table used by the algorithm. The name of the table to be used in the query is `netlytics`.
You must specify which features to use, separately for numerical and categorical. Categorical features are encoded in a one-hot vector, so do not provide features with high cardinality.
You can normalize the data before running the algorithm.

The output of clustering is the input Data Table, with two additional columns: one reporting the employed features, and the other specifying the cluster ID.

To run clustering, you must use the `run_clustering.py` script, with the following syntax:
```
spark-submit run_clustering.py [-h] [--connector connector]
                         [--input_path input_path] [--start_day start_day]
                         [--end_day end_day] [--output_path output_path]
                         [--algo algo] [--params params] [--query query]
                         [--numerical_features numerical_features]
                         [--categorical_features categorical_features]
                         [--normalize]

optional arguments:
  -h, --help            show this help message and exit
  --connector connector
                        Connector class name
  --input_path input_path
                        Base Log Files Input Path
  --start_day start_day
                        Start day for analysis, format YYYY_MM_DD
  --end_day end_day     End day for analysis, format YYYY_MM_DD
  --output_path output_path
                        Path where to store resulting labeled Data Table
  --algo algo           Clustering Algorithm to run
  --params params       Parameters to be given to the Clustering Algorithm, in
                        Json
  --query query         Eventual SQL query to execute to preprocess the
                        dataset
  --numerical_features numerical_features
                        Columns to use as numerical features, separated by
                        comma
  --categorical_features categorical_features
                        Columns to use as categorical features, separated by
                        comma
  --normalize           Normalize data before clustering
```

For example, to run a simple clustering on **Squid** logs, you may run:
```
spark-submit run_clustering.py \
      \
      --connector "connectors.squid_to_HTTP.Squid_To_HTTP" \
      --input_path "logs/squid" \
      --start_day "2017_03_01" \
      --end_day "2017_03_31" \
      \
      --algo "algos.clustering.KMeans.KMeans" \
      --params '{"K":5}' \
      --categorical_features "method,status_code,content_type,s_port"
      --numerical_features "response_body_len"
      --normalize
      --output_path "results_clustering" \
```

Available algorithms are:
* **KMeans**
    * Parameters: `K` and `seed`
    * Class Name: `algos.clustering.KMeans.KMeans`
* **BisectingKMeans**
    * Parameters: `K` and `seed`
    * Class Name: `algos.clustering.BisectingKMeans.BisectingKMeans`
* **GaussianMixture**
    * Parameters: `K` and `seed`
    * Class Name: `algos.clustering.GaussianMixture.GaussianMixture`
* **DBScan**
    * Parameters: `eps` and `min_points`
    * Class Name: `algos.clustering.DBScan.DBScan`
    * **Warning**: the provided implementation is parallel, but DBScan is an O(n<sup>2</sup>) algorithm, and, thus, the required computational power (and memory) is still polynomial with respect to the input dataset size.


Other algorithms will be added in the future.

## 3.2 Anomaly Detection
Anomaly Detection Algorithms discover unusual/uncommon/infrequent instances in the data.
You can run Anomaly Detection Algorithms over a Data Table, using the `run_anomaly_detection.py` script.
Its syntax is:
```
spark-submit run_anomaly_detection.py [-h] [--connector connector]
                                [--input_path input_path]
                                [--start_day start_day] [--end_day end_day]
                                [--output_path output_path] [--algo algo]
                                [--params params] [--query query]
                                [--numerical_features numerical_features]
                                [--categorical_features categorical_features]
                                [--normalize]

optional arguments:
  -h, --help            show this help message and exit
  --connector connector
                        Connector class name
  --input_path input_path
                        Base Log Files Input Path
  --start_day start_day
                        Start day for analysis, format YYYY_MM_DD
  --end_day end_day     End day for analysis, format YYYY_MM_DD
  --output_path output_path
                        Path where to store resulting labeled Data Table
  --algo algo           Clustering Algorithm to run
  --params params       Parameters to be given to the Clustering Algorithm, in
                        Json
  --query query         Eventual SQL query to execute to preprocess the
                        dataset
  --numerical_features numerical_features
                        Columns to use as numerical features, separated by
                        comma
  --categorical_features categorical_features
                        Columns to use as categorical features, separated by
                        comma
```
For example, to find anomalous spikes in data, you can run the following command:
```
spark-submit run_anomaly_detection.py \
      \
      --connector "connectors.squid_to_HTTP.Squid_To_HTTP" \
      --input_path "logs/squid" \
      --start_day "2017_03_01" \
      --end_day "2017_03_31" \
      \
      --algo algos.anomaly_detection.S_H_ESD.S_H_ESD \
      --query 'select FLOOR(time_start/60)*60 as time, sum(s_bytes) as value from netlytics group by FLOOR(time_start/60) * 60 order by time'
      --numerical_features "value" \
      --output_path "S_H_ESD_output" \
```

Available algorithms are:
* **Seasonal Hybrid ESD (S-H-ESD)**: is an angorithm for detecting anomalies on time series proposed by Twitter (info [here](https://github.com/twitter/AnomalyDetection) and [here](https://arxiv.org/pdf/1704.07706.pdf)). You must provide a data table with a column named `time` and a single feature representing the value of the series.
S-H-ESD is able to find both *local* and *global* anomalies, as exemplified by the next figure, where we used 1 month data coming from operational passive probes.
![alt text](https://github.com/marty90/netlytics/raw/master/images/timeseries.png)
    * Parameters:
        * `period`: periodicity of the data, If not specified, it is autcomputed.
        * `alpha`: confidence interval for generalized-ESD. Default is 0.025
        * `hybrid`: if True, median and MAD replace mean and std. Default is True.
    * Class Name: `algos.anomaly_detection.S_H_ESD.S_H_ESD`

* **Univariate Anomaly Detection**: detect anomalies on a single numerical feature. It provides two possibilities to detect outliers: (i) outliers are instances lying outside mean +- 3 * stdev, or (ii) instances lying outside [Quartile1 - 1.5 * IQR, Quartile3 + 1.5 * IQR], also known as *boxplot rule*. You must provide a single numerical feature to this algorothm.
    * Parameters:
        * `method`: can be gaussian or boxplot, to select algorithm to use.
    * Class Name: `algos.anomaly_detection.univariate_anomalies.UnivariateAnomalies`

## 3.3 Advanced Analytics

Advanced analytics solve particular problems in networking. Available algorithms are:
* **REMeDy: DNS manipulation detection**: REMeDy is a system that assists operators to identify the use of rogue DNS resolvers in their networks. It is a completely automatic and parameter-free system that evaluates the consistency of responses across the resolvers active in the network. The paper is available [here](https://www.dropbox.com/s/8s9azpvnqsle78t/rogue_DNS_ds4n.pdf?dl=1).
    * Input Data Table: DNS
    * Class Name: `algos.remedy.Remedy_DNS_manipulations.RemedyDNSManipulations`
    * Parameters: 
        * min_resolutions : Minimum number of observation to consider a domain. Default 50.
        * ASN_VIEW : Path to an updated ASN view compatible with `pyasn` python module.
     * Output: A single CSV file reporting general per-resolver statistics, along with discovered manipulations.
* **WHAT: Cloud service meter**: WHAT is a system to uncover the overall traffic produced by specific web services. WHAT combines big data and machine learning approaches to process large volumes of network flow measurements and learn how to group traffic due to pre-defined services of interest. The paper is available [here](http://www.tlc-networks.polito.it/mellia/papers/BMLIT_web_meter.pdf).
    * Input Data Table: Named Flows
    * Class Name: `algos.WHAT.WHAT`
    * Parameters: 
        * CORES : Core Domains to measure.
        * OW : Observation Windows for training, in seconds. Default 10.
        * GAP : Silent time for considering OW, in seconds. Default 10.
        * EW : Evaluation Window for classification, in seconds. Default 5.
        * MINFREQ : Minimum Frequency for a domain to remain in tha BoD. Default 0.05.
        * N_TOP : Truncate output list after N_TOP entries. Default 50.
     * Output: A single CSV file reporting the amount of traffic due to each core domain.
* **PAIN: Passive Indicator**: PAIN (PAssive INdicator) is an automatic system to observe the performance of web pages at ISPs. It leverages passive flow-level and DNS measurements which are still available in the network despite the deployment of HTTPS. With unsupervised learning, PAIN automatically creates a model from the timeline of requests issued by browsers to render web pages, and uses it to analyze the web performance in real-time.. The paper is available [here](http://porto.polito.it/2675141/2/ssl_qoe_tma_open.pdf).
    * Input Data Table: Named Flows
    * Class Name: `algos.pain.pain.Pain`
    * Parameters: 
        * CORES : Core Domains to measure.        
        * GAP : Silent time for considering OW, in seconds. Default 10.
        * EW : Evaluation Window for classification, in seconds. Default 30.
        * MINFREQ : Minimum Frequency for a domain to remain in tha BoD. Default 0.25.
        * N_CP : Number of CheckPoints for the model, default is 4.
     * Output: A single CSV file reporting the an entry for each visit to a core domain, along with performance metrics.
* **Contacted domains**: account the traffic generated on the network on a per domain fashion.
    * Input Data Table: Named Flows
    * Class Name: `algos.domain_traffic.DomainTraffic`
    * Parameters: 
        * N_TOP :Truncate output list after N_TOP entries. Default 50.
     * Output: A single CSV file reporting the amount of traffic due to each domain.
* **Save Dataset**: Simply save the dataframe in local.
    * Input Data Table: * (all)
    * Class Name: `algos.save_Dataframe.SaveDataFrame`
    * Parameters: 
        * N : Truncate output after N entries. Default: No limit.
     * Output: The dataframe in CSV format.

To run a NetLytics analytic, you shoud use the `run_job.py` script, which has the following syntax:
```
spark2-submit run_job.py [-h] [--input_path input_path] [--output_path output_path]
                  [--connector connector] [--algo algo] [--params params]
                  [--start_day start_day] [--end_day end_day]
                  [--temp_dir_local temp_dir_local]
                  [--temp_dir_HDFS temp_dir_HDFS]
                  [--persistent_dir_local persistent_dir_local]
                  [--persistent_dir_HDFS persistent_dir_HDFS]

optional arguments:
  -h, --help            show this help message and exit
  --input_path input_path
                        Base Log Files Input Path
  --output_path output_path
                        Directory where the output of the algo is stored
  --connector connector
                        Connector class name
  --algo algo           Algorithm to run
  --params params       Parameters to be given to the Algo, in Json
  --start_day start_day
                        Start day for analysis, format YYYY_MM_DD
  --end_day end_day     End day for analysis, format YYYY_MM_DD
  --temp_dir_local temp_dir_local
                        Directory where to store intermediate files
  --temp_dir_HDFS temp_dir_HDFS
                        Directory on HDFS where to store intermediate files
  --persistent_dir_local persistent_dir_local
                        Directory where to store persistent algorithm data
                        (local)
  --persistent_dir_HDFS persistent_dir_HDFS
                        Directory where to store persistent algorithm data
                        (HDFS)
```
Recall that the script must be submitted to spark, and, thus, executed with the `spark-submit` utility.

In this example we run the command to account the account the traffic to the corresponding domain for the whole month.

```
spark-submit run_job.py \
      \
      --connector "connectors.squid_to_named_flows.Squid_To_Named_Flows" \
      --input_path "logs/squid" \
      --start_day "2017_03_01" \
      --end_day "2017_03_31" \
      \
      --algo "algos.domain_traffic.DomainTraffic" \
      --params '{"N":40}' \
      --output_path "results_DOMAIN_TRAFFIC" \
```

## 3.4 Running SQL queries

NetLyitics allows to run SQL queries on Data Tables to perform simple analytics. Instruction to this are reported later.
You must use the `run_query.py` script, which has the following syntax:  
```
spark2-submit run_query.py [-h] [--input_path input_path]
                    [--output_file_local output_file_local]
                    [--output_file_HDFS output_file_HDFS] [--query query]
                    [--connector connector] [--start_day start_day]
                    [--end_day end_day]

optional arguments:
  -h, --help            show this help message and exit
  --input_path input_path
                        Base Log Files Input Path
  --output_file_local output_file_local
                        File where the resulting table is saved (locally).
                        Cannot be specified together with output_file_HDFS
  --output_file_HDFS output_file_HDFS
                        File where the resulting table is saved (HDFS). Cannot
                        be specified together with output_file_local
  --query query         SQL Query to exectute. Use "netlytics" as SQL table
                        name
  --connector connector
                        Connector class name
  --start_day start_day
                        Start day for analysis, format YYYY_MM_DD
  --end_day end_day     End day for analysis, format YYYY_MM_DD

```

To get the list of the available columns, see the `json` schemas in the `schema` directories.
The name of the table to be used in the query is `netlytics`.

Now, we perform the same job of above using a SQL query, and the `run_query.py` script.
```
spark-submit run_query.py \
      \
      --connector "connectors.squid_to_named_flows.Squid_To_Named_Flows" \
      --input_path "logs/squid" \
      --start_day "2017_03_01" \
      --end_day "2017_03_31" \
      \
      --query "SELECT name, SUM(s_bytes) AS traffic FROM netlytics GROUP BY name ORDER BY SUM(s_bytes)" \
      --output_file_local "traffic_name.csv"

```

# 4. Use NetLytics as a library
Many parts of NetLytics can be used as a library, and be included in you spark application.
All you need is to include the needed modules and use them.

## 4.1 Get Data Table from raw logs
You can create a Data Table (a Spark Dataframe) using NetLytics APIs.
You must use the function: `core.utils.get_dataset()`.

For example, you may run:
```
import core.utils

# Create Spark Context
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# Get path of NetLytics
base_path = os.path.dirname(os.path.realpath(__file__))
    
# Retrieve a Data Frame from raw log files in HDFS
dataframe = core.utils.get_dataset(sc,\
                                   spark,\
                                   base_path,\
                                   "connectors.squid_to_named_flows.Squid_To_Named_Flows",\
                                   "logs/squid",\
                                   "2017_03_01", "2017_03_31 )
```

## 4.2 Process a Data Table to extract features
You can do some operation to a Data Table using the `core.utils.transform()`.
You can specify: (i) a SQL query to execute on the Data Frame, (ii) which categorical and numerical features to extract, and (iii) whether to normalize the feature.
Note that categorical features are encoded in a one-hot vector, so do not provide features with high cardinality.
The name of the table to be used in the SQL query is `netlytics`.
 
The output DataFrame has an extra column called `features`, containing a list of floating numbers to be used as features to be used for machine learning algorithms.

For example, you may run:
```
import core.utils

# Create a Data Table
dataframe=...

# Manipulate it
manipulated_dataset = core.utils.transform(dataframe,spark,\
                                           sql_query = "SELECT * from netlytics",\
                                           numerical_features = ["response_body_len"],
                                           categorical_features = ["method","status_code"],
                                           normalize=True,
                                           normalize_p=2)

```

## 4.3 Use algorithms to process Data Tables
You can use available algorithms in your own code.

### 4.3.1 Clustering
Each class implementing a clustering algorithm extends the class `core.clustering_algo.ClusteringAlgo`.
It provides a constructor with a single argument being a dictionary of parameters, a single method called `run()` that takes as input a DataFrame and provides as output another DataFrame with an extra column specifying the cluster ID.
The input DataFrame must have a column named `features` containing a list of floats to be used as features.

Example:
```
from algos.clustering.KMeans import KMeans

# You must craft a dataframe with the 'features' column
dataframe = ...

kmeans = KMeans ({"K":10, "seed":1234})
prediction = kmeans.run(dataframe)

```

### 4.3.2 Anomaly Detection
Each class implementing an anomaly detection algorithm extends the class `core.anomaly_detection_algo.AnomalyDetectionAlgo`.
It provides a constructor with a single argument being a dictionary of parameters, a single method called `run()` that takes as input a DataFrame and provides as output another a **pandas** dataframe with the results of the run.
Output format may variate according to the algorithm.

Example:
```
from algos.anomaly_detection.S_H_ESD import S_H_ESD

# You must craft a dataframe with the 'time' and 'features' columns
dataframe = ...

# Run the S_H_ESD
s_h_esd = S_H_ESD ()
anomalies = s_h_esd.run(dataframe)

# Save the output pandas dataframe
anomalies.to_csv("anomalies.csv")
```

### 4.3.3 Advanced Analytics
Each class implementing an Advanced Analytics extends the class `core.algo.Algo`.
It takes as input a DataFrame of one of the aforementioned types (DNS, HTTP and NamedFlows).

It provides a constructor with a several arguments:
* **input_DF**: the input data table to manipulate
* **output_dir**: the input directory (local) where results are stored
* **temp_dir_local**: a temporary local directory where the algorithm may save temporary files.
* **temp_dir_HDFS**: a temporary HDFS directory where the algorithm may save temporary files.
* **persistent_dir_local**: a local directory where the algorithm may save data for further runs.
* **persistent_dir_HDFS**: a HDFS directory where the algorithm may save data for further runs.

Each algorithm has one method called `run()` which executes the algorithm and saves resulting output.


Example:
```
from algos.WHAT import WHAT

# You must craft a NamedFlows dataframe
dataframe = ...

# Get an instance of the algorithm
what = WHAT (dataframe,\
            "/output_WHAT",
            "/tmp",
            "/data/user/foo/tmp",\
            "~/what_data",\
            "/data/user/foo/what_data")

# Run it
what.run()
```

