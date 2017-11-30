from core.clustering_algo import ClusteringAlgo
import numpy as np
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import Row
import networkx as nx
from collections import defaultdict

REPARTITION_RATIO=1000

# DNS Manipulations
class DBScan(ClusteringAlgo):

    # Class Information
    _name = "DBScan"
    parameters={"eps":0.25,"min_points":10, "metric":"euclidean"}

    # Run the algorithm
    def run(self, input_dataframe):

        eps = self.parameters["eps"]
        min_points = self.parameters["min_points"]
        metric = self.parameters["metric"]
        dbscan = Spark_DBScan(eps=eps, min_points=min_points, metric=metric)

        dbscan.fit(input_dataframe)        
        prediction = dbscan.transform(input_dataframe)
        return prediction



class Spark_DBScan():

    def __init__ (self, eps=0.25, min_points=10,metric="euclidean"):
        self._eps        = eps
        self._min_points = min_points
        self.metric = metric

    def fit (self, input_dataframe):

        eps = self._eps
        min_points = self._min_points

        points = [ np.array(e["features"]) for e in input_dataframe.select("features").collect()]
        distance = lambda e1, e2 : np.linalg.norm(e1 - e2)

        sc = input_dataframe.rdd.context

        # Create Distributed Elements
        # Get a vector like:
        # ( 1 , [e1, e2, ... ])
        # ( 2 , [e1, e2, ... ])
        points_rdd = sc.parallelize( range(len(points))).repartition(int(len(points)/REPARTITION_RATIO)+1)
        def join_points(i,points):
            return (i,points)      
        distributed_elements = points_rdd.map(lambda i : join_points(i,points) )
        
        # Create Distance Matrix
        # ( 1 , [d1, d2, ... ])
        # ( 2 , [d1, d2, ... ])
        def calc_distance (tup):
            point_id, points = tup
            point_val = points [point_id]
            distances = [ distance( point_val, p) for p in points ]
            return (point_id, distances)
        distributed_distance = distributed_elements\
                              .map ( calc_distance ) 

        # Get eps-neighborhood as mask
        # Get a vector like:
        # ( 1 , [1, 0, ... ])
        # ( 2 , [1, 1, ... ])
        distributed_distance_thresholded = distributed_distance\
                                    .map ( lambda tup : (tup[0], [0 if d > eps else 1 for d in tup[1]   ] ))

        # Tranform eps-neighborhood in a set
        #   cores   all_neigh
        # [( {1}, {1,3,4 ... }) ]
        # [( {2}, {2,8,9 ... }) ]
        distributed_sets=distributed_distance_thresholded\
                              .map( lambda tup : [ [ {tup [0]}, {i for i, e in enumerate(tup[1]) if e != 0} ]] )

        # Filter Elements with less than min_points
        distributed_sets_filtered=distributed_sets.filter(lambda tup: len(tup[0][1])>=min_points)

        # Reduce and get clusters
        clusters = distributed_sets_filtered.reduce( merge_clusters )

        # Label points with cluster ID
        point_to_cluster = [ [p,-1] for p in points]
        for i,(this_cores, this_neigh) in enumerate(clusters):
            for p in this_neigh:
                point_to_cluster[p][1] = i

        self._model = point_to_cluster

        return point_to_cluster

    def transform(self, dataframe):

        spark = dataframe.sql_ctx

        dataframe = dataframe.repartition(int(len(self._model)/REPARTITION_RATIO)+1)

        cluster_ids = spark.createDataFrame( [ (c,) for p,c in self._model ] )\
                            .repartition(int(len(self._model)/REPARTITION_RATIO)+1)

        def merge_rows(tup):
            row1, row2 = tup
            d1 = row1.asDict()
            d1.update(row2.asDict())
            new_row =    Row (**d1)   
            return new_row

        joined = dataframe.rdd.zip(cluster_ids.rdd).map(merge_rows).toDF(sampleRatio=1)\
                              .withColumnRenamed("_1","prediction")


        return joined




def merge_clusters ( c1, c2 ):

    input_clusters = c1 + c2

    # 1. Update cores

    # Get all cores
    cores_updated = set()
    for this_cores, this_neigh in input_clusters:
        cores_updated |= this_cores

    # Update in cluster list
    for i, (this_cores, this_neigh) in enumerate(input_clusters):
        input_clusters[i][0] = this_neigh & cores_updated


    # 2. Decide which clusters to merge
    
    # Map core to its clusters
    core_to_clusters = defaultdict(set)
    for i, (this_cores, this_neigh) in enumerate(input_clusters):
        for c in this_cores:
            core_to_clusters[c].add(i)

    # Define merge list: which clusters to merge
    clusters_to_merge=defaultdict(set)
    for i, (this_cores, this_neigh) in enumerate(input_clusters):
        for c in this_cores:
            clusters_to_merge[i] |= core_to_clusters[c]

    # Calculate Connected Components
    graph=nx.Graph(clusters_to_merge)    
    components = list(nx.connected_components(graph))
    old_to_new = {}
    for new_id,s in enumerate(components):
        for old_id in s:
            old_to_new[old_id]=new_id



    # 3. Merge the clusters

    # Create empy data strucutre
    new_clusters=[]
    for i in range(len(components)):
        new_clusters.append( [set(),set()] )

    for i, (this_cores, this_neigh) in enumerate(input_clusters):
        new_id = old_to_new[i]
        new_clusters[new_id][0] |= this_cores
        new_clusters[new_id][1] |= this_neigh


    return new_clusters

