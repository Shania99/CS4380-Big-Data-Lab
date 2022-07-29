import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,decode,substring
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.sql import Row
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


brokers = '10.150.0.2:9092'
topic = 'iris_pred'
model_path = "gs://bdl22_ch18b067/pipeline_model"

spark = SparkSession \
        .builder \
        .appName("IrisSub") \
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")



df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", brokers) \
  .option("subscribe", topic) \
  .load()

schema = StructType() \
  .add("sepal_length", FloatType()) \
  .add("sepal_width", FloatType()) \
  .add("petal_length", FloatType()) \
  .add("petal_width", FloatType()) \
  .add("species", StringType())


df = df.select(f.from_json(f.decode(df.value, 'utf-8'), schema=schema).alias("input"))
df = df.select("input.*") 


df_d = df.drop('sepal_width')



model = PipelineModel.load(model_path)
df_predictions = model.transform(df_d)
df_predictions = df_predictions.select(df_predictions.pred_species, df_predictions.species, df_predictions.prediction, df_predictions.label)

def batch_func(df, epoch):
  evaluator = MulticlassClassificationEvaluator(predictionCol='prediction',labelCol='label',metricName='accuracy')
  acc = evaluator.evaluate(df)*100 
  acc_row = Row(ba=acc)
  acc_df = spark.createDataFrame([acc_row])
  acc_col = f"Batch {epoch} Accuracy"
  acc_df = acc_df.withColumnRenamed('ba',acc_col)
  df_lab = df.select(df.pred_species, df.species)
  first_col_str = f"Batch {epoch} predicted species" 
  sec_col_str = f"Batch {epoch} true species" 
  df_lab =df_lab.withColumnRenamed('pred_species',first_col_str)
  df_lab =df_lab.withColumnRenamed('species',sec_col_str)
  df_lab.write.format("console").save()
  acc_df.write.format("console").save()

query = df_predictions \
        .writeStream \
        .option("truncate",False) \
        .foreachBatch(batch_func) \
        .start() \

"""
query = df \
        .writeStream \
        .format("console") \
        .option("truncate",False) \
        .start()
"""
query.awaitTermination()
