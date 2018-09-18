from pyspark import RDD
from pyspark.sql import SparkSession
from graphframes import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Triangles").getOrCreate()
    sc = spark.sparkContext
    edges: RDD = sc.textFile("./data/graph.txt") \
        .map(lambda l: l.split()).map(lambda p: (int(p[0]), int(p[1]))) \
        .filter(lambda v: v[0] != v[1]).map(lambda v: v if v[0] < v[1] else (v[1], v[0])) \
        .map(lambda v: (v[0], v[1], 1)).cache()
    vertices = edges.flatMap(lambda v: [v[0], v[1]])
    g = GraphFrame()
