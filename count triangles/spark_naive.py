from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Triangles").getOrCreate()
    sc = spark.sparkContext
    edges: RDD = sc.textFile("./data/graph.txt")
    edges = edges.map(lambda l: l.split()).map(lambda p: (int(p[0]), int(p[1]))) \
        .filter(lambda v: v[0] != v[1]).map(lambda v: v if v[0] < v[1] else (v[1], v[0])).distinct().cache()
    abFields = [StructField("A", IntegerType(), False), StructField("B", IntegerType(), False)]
    abSchema = StructType(abFields)
    ab = spark.createDataFrame(edges, abSchema)
    bc1Fields = [StructField("B", IntegerType(), False), StructField("C1", IntegerType(), False)]
    bc1Schema = StructType(bc1Fields)
    bc1 = spark.createDataFrame(edges, bc1Schema)
    ac2Fields = [StructField("A", IntegerType(), False), StructField("C2", IntegerType(), False)]
    ac2Schema = StructType(ac2Fields)
    ac2 = spark.createDataFrame(edges, ac2Schema)
    abc1c2 = ab.join(bc1, "B").join(ac2, "A")
    print(abc1c2.filter("C1 = C2").count())
