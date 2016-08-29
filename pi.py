from random import random
import sys
from pyspark.sql import SparkSession

def sample(p):
    x, y = random(), random()
    return 1 if x*x + y*y < 1 else 0

if __name__ == "__main__":
    spark = SparkSession.builder.appName("crobby-Pi-1").getOrCreate()
    
    parts = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * parts
    print "Will be using %d partitions." % (parts)
    count = spark.sparkContext.parallelize(range(0, n), parts).map(sample).reduce(add)
    print "Pi is still approximately %f" % (4.0 * count / n)

