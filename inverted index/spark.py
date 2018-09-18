import os
import re

from pyspark import SparkContext
from pyspark.rdd import PipelinedRDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Inverted index").getOrCreate()
    sc: SparkContext = spark.sparkContext

    header_re = re.compile(r"[\w-]+:.+")
    word_re = re.compile(r"([a-zA-Z']{2,})")


    def remove_headers(content):
        for line in content.split("\n"):
            if line == "\n" or header_re.fullmatch(line) is not None:
                continue
            for word in word_re.findall(line):
                yield word.lower()


    index: PipelinedRDD = sc.wholeTextFiles("./data/20_newsgroup/*/*").cache() \
        .map(lambda x: (os.path.split(x[0])[1], x[1])) \
        .flatMap(
        lambda x: [(word, (1, [x[0]])) for word in remove_headers(x[1])]) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    index.map(lambda x: f"{x[0]},{x[1][0]},{' '.join(x[1][1])}").saveAsTextFile("./out/output.txt")
