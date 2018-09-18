from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Triangles").getOrCreate()
    sc = spark.sparkContext
    edges: RDD = sc.textFile("./data/graph.txt").map(lambda l: l.split()).map(lambda p: (int(p[0]), int(p[1]))).filter(
        lambda v: v[0] != v[1]).map(lambda v: v if v[0] < v[1] else (v[1], v[0])).cache()
    abFields = [StructField("A", IntegerType(), False), StructField("B", IntegerType(), False)]
    abSchema = StructType(abFields)
    ab = spark.createDataFrame(edges, abSchema)
    bc1Fields = [StructField("B", IntegerType(), False), StructField("C1", IntegerType(), False)]
    bc1Schema = StructType(bc1Fields)
    bc1 = spark.createDataFrame(edges, bc1Schema)
    edgeListBc = sc.broadcast(set(edges.toLocalIterator()))
    abc1 = ab.join(bc1, "B")
    from pyspark.sql.functions import udf


    def isTriangle(src, dst):
        return (src, dst) in edges


    udfIsTriangle = udf(isTriangle, BooleanType())
    abc1.withColumn("isTriangle", udfIsTriangle("A", "C1")).groupBy("isTriangle").count()
