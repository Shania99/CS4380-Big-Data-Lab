from __future__ import absolute_import
from time import sleep
import pyspark
import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import *


brokers = '10.150.0.2:9092'
topic = 'iris_pred'
data_file = "iris.csv"


prod = KafkaProducer(bootstrap_servers=[brokers],
                         value_serializer=lambda x: 
                         x.encode('utf-8') 
                         )

spark = SparkSession \
    .builder \
    .appName("IrisPub") \
    .getOrCreate()


schema = StructType() \
  .add("sepal_length", FloatType()) \
  .add("sepal_width", FloatType()) \
  .add("petal_length", FloatType()) \
  .add("petal_width", FloatType()) \
  .add("species", StringType())


iris_data = spark.read.csv(data_file,header=True,schema=schema)

count = 0

for row in iris_data.toJSON().collect():
    prod.send(topic, value=row)
    sleep(1)
    # # TEST CODE
    # count += 1
    # if count > 3:
    #     break
