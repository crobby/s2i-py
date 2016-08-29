from random import random
import sys
from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf()
conf.setAppName('spark-pi-s2i-crobby')
sc = SparkContext(conf=conf)


def sample(p):
    x, y = random(), random()
    print ("returning %s, %s" % x, y)
    return 1 if x*x + y*y < 1 else 0

NUM_SAMPLES=int(sys.argv[1])
print "Will be using %s as NUM_SAMPLES" % NUM_SAMPLES

count = sc.parallelize(xrange(0, NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b)
print "Pi is still approximately %f" % (4.0 * count / NUM_SAMPLES)




