from pyspark.sql import SparkSession
from pyspark.broadcast import Broadcast
from pyspark import SparkContext
import math
import sys



def emit_triangles(neighbors):
    triangles = []
    for i in range(len(neighbors)):
        for j in range(i+1, len(neighbors)):
            triangles.append((neighbors[i], neighbors[j]))
    return triangles


def find_triangles(node, neighbors, adjacency_list_bc):
    adjacency_list = adjacency_list_bc.value
    count = 0

    for neighbor in neighbors:

        if neighbor in adjacency_list:
            for neighbor_of_neighbor in adjacency_list[neighbor]:

                if neighbor_of_neighbor in neighbors and neighbor_of_neighbor != node:

                    if node < neighbor and node < neighbor_of_neighbor:
                        count += 1
    return (count//2)



def heavy_hitters(edg):

  list_adj = edg.map(parse_edge).flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda a, b: a + b)
  edg_cnt = edg.count()
  limit = int(math.sqrt(edg_cnt))
  return list_adj.filter(lambda x: len(x[1]) >= limit).count()
  



def parse_edge(row):
    return (int(row.split()[0]), int(row.split()[1]))


if __name__ == "__main__":

  # Initialize SparkSession
  sc = SparkContext(appName="Triangles")

  edg = sc.textFile(sys.argv[1])

  list_adj = edg.map(parse_edge).flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda a, b: a + b)

  triangles_rdd = list_adj.flatMap(lambda x: emit_triangles(x[1])).flatMap(lambda x: [(x, 1)]).reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] == 2)
  adjacency_list = sc.broadcast(list_adj.collectAsMap())

  hhc = heavy_hitters(edg)
  triangles_number = list_adj.map(lambda x: find_triangles(x[0], x[1], adjacency_list)).reduce(lambda a, b: a + b)

  # Print the results
  print("No of heavy hitter nodes:", hhc)
  print("No of triangles:", triangles_number)