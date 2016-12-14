import json

from pyspark.sql import SparkSession
#

from pyspark import SparkContext

def transformation(r):
    return r['data']['customer_id'], r['data']['timestamp'], r['data']['properties']

def derive_attribute(data):
    customer_id, events = data
    return customer_id, len(events)

sc = SparkContext(master="local", appName="Simple App")
rdd = sc.textFile("session_start/2016-10-2*")
rdd = rdd.map(lambda x: json.loads(x)).map(transformation).groupBy(lambda x: x[0]).map(derive_attribute)
for r in rdd.take(50):
    print(r)
#print(rdd.count())









# spark = SparkSession \
#     .builder \
#     .appName("Python Spark SQL basic example") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()
#
#
# df = spark.read.json("session_start/*")
# df = df.select('data.*').select('customer_id', 'properties.*')
#
# print(df.count())
# print(df.show())
#
#
