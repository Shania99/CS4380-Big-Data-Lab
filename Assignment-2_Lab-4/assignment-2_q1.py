import pyspark
import sys

if len(sys.argv) != 3:
  raise Exception("Inputs required: <input_uri> <output_uri>")

input_uri=sys.argv[1]
output_uri=sys.argv[2]

sc = pyspark.SparkContext()
data = sc.textFile(sys.argv[1])
header = data.first()
data.filter(lambda line: line != header)
t = data.map(lambda line: line.split()[1])
hrs = t.map(lambda time: int(time[:2]))

def concat(hr):
  if hr < 6:
    return ("0-6", 1)
  elif hr < 12:
    return ("6-12", 1)
  elif hr < 18:
    return ("12-18", 1)
  else:
    return ("18-24", 1)


hrCounts = hrs.map(concat).reduceByKey(lambda x, y: x+y)
hrCounts.saveAsTextFile(sys.argv[2])




