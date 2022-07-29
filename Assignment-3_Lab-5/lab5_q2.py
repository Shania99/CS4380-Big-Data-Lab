from __future__ import print_function
from pyspark.context import SparkContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
import numpy as np
from pyspark.ml.feature import StandardScaler
import pyspark.sql.functions as f
import pyspark.sql.types
from pyspark.sql import Row
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

sc = SparkContext()
spark = SparkSession(sc)

########## Reading Dataframe
spark_df = spark.read.format("bigquery").option("table", "lab5.iris_dataset").load().toDF("sl", "sw", "pl", "pw", "qcolumn")

clean_data = spark_df.withColumn("label", spark_df["qcolumn"])
cols = spark_df.drop('qcolumn').columns
assembler = VectorAssembler(inputCols=cols, outputCol = 'features')
labelIndexer = StringIndexer(inputCol="qcolumn", outputCol="indexed_label").fit(spark_df)

######### Standardize the columns
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)

######### Splitting Data
(data_train, data_test) = spark_df.randomSplit([0.8, 0.2])

######## Training a RandomForest model
rf = RandomForestClassifier(labelCol="indexed_label", featuresCol="scaled_features", numTrees=10)

####### Retrieve orginal labels from indexed labels
label_conv = IndexToString(inputCol="prediction", outputCol="predicted_label",
labels=labelIndexer.labels)
pipeline = Pipeline(stages=[labelIndexer, assembler,scaler, rf, label_conv])

####### Train the Random Forest Model model
model = pipeline.fit(data_train)

####### prediction
pred = model.transform(data_test)
evaluator = MulticlassClassificationEvaluator(
label_col="indexed_label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(pred)
print("Training Accuracy for Random Forest = %g" % (accuracy))


######## Training a Logistic Regression model
lr = LogisticRegression()

####### Retrieve orginal labels from indexed labels
label_conv = IndexToString(inputCol="prediction", outputCol="predicted_label",
labels=labelIndexer.labels)
pipeline = Pipeline(stages=[labelIndexer, assembler,scaler, lr, label_conv])

####### Train the Logistic Regression model
model = pipeline.fit(data_train)

####### prediction
pred = model.transform(data_test)
evaluator = MulticlassClassificationEvaluator(
labelCol="indexed_label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(pred)
print("Training Accuracy for Logistic Regression = %g" % (accuracy))








