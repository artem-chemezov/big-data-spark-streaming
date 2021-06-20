import os
import socket
from bisect import bisect
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
import pyspark.sql.functions as F
from jinja2 import Environment, FileSystemLoader

if "HADOOP_CONF_DIR" in os.environ:
    del os.environ["HADOOP_CONF_DIR"]
    
"HADOOP_CONF_DIR" in os.environ

APP_NAME = "producer"
NORMALIZED_APP_NAME = APP_NAME.replace('/', '_').replace(':', '_')
APPS_TMP_DIR = os.path.join(os.getcwd(), "tmp")
APPS_CONF_DIR = os.path.join(os.getcwd(), "conf")
APPS_LOGS_DIR = os.path.join(os.getcwd(), "logs")
LOG4J_PROP_FILE = os.path.join(APPS_CONF_DIR, "pyspark-log4j-{}.properties".format(NORMALIZED_APP_NAME))
LOG_FILE = os.path.join(APPS_LOGS_DIR, 'pyspark-{}.log'.format(NORMALIZED_APP_NAME))
EXTRA_JAVA_OPTIONS = f"-Dlog4j.configuration=file://{LOG4J_PROP_FILE} -Dspark.hadoop.dfs.replication=1 -Dhttps.protocols=TLSv1.0,TLSv1.1,TLSv1.2,TLSv1.3"
LOCAL_IP = socket.gethostbyname(socket.gethostname())

for directory in APPS_CONF_DIR, APPS_LOGS_DIR, APPS_TMP_DIR:
    if not os.path.exists(directory):
        os.makedirs(directory)
        
env = Environment(loader=FileSystemLoader('/opt'))
template = env.get_template("pyspark_log4j.properties.template")
template.stream(logfile=LOG_FILE).dump(LOG4J_PROP_FILE)


spark = SparkSession\
    .builder\
    .appName(APP_NAME)\
    .master("k8s://https://10.32.7.103:6443")\
    .config("spark.driver.host", LOCAL_IP)\
    .config("spark.driver.bindAddress", "0.0.0.0")\
    .config("spark.executor.instances", "2")\
    .config("spark.executor.cores", '3')\
    .config("spark.memory.fraction", "0.8")\
    .config("spark.memory.storageFraction", "0.6")\
    .config("spark.executor.memory", '2g')\
    .config("spark.driver.memory", "2g")\
    .config("spark.driver.maxResultSize", "1g")\
    .config("spark.kubernetes.memoryOverheadFactor", "0.3")\
    .config("spark.driver.extraJavaOptions", EXTRA_JAVA_OPTIONS)\
    .config("spark.kubernetes.namespace", "achemezov-309974")\
    .config("spark.kubernetes.driver.label.appname", APP_NAME)\
    .config("spark.kubernetes.executor.label.appname", APP_NAME)\
    .config("spark.kubernetes.container.image", "node03.st:5000/spark-executor:achemezov-309974")\
    .config("spark.local.dir", "/tmp/spark")\
    .config("spark.sql.streaming.checkpointLocation", "/home/jovyan/nfs-home/tmp") \
    .config("spark.driver.extraClassPath", "/home/jovyan/shared-data/my-project-name-jar-with-dependencies.jar")\
    .config("spark.executor.extraClassPath", "/home/jovyan/shared-data/my-project-name-jar-with-dependencies.jar")\
    .config("spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-tmp-spark.mount.path", "/tmp/spark")\
    .config("spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-tmp-spark.mount.readOnly", "false")\
    .config("spark.kubernetes.executor.volumes.hostPath.depdir.mount.path", "/home/jovyan/shared-data")\
    .config("spark.kubernetes.executor.volumes.hostPath.depdir.options.path", "/nfs/shared")\
    .config("spark.kubernetes.executor.volumes.hostPath.depdir.options.type", "Directory")\
    .config("spark.kubernetes.executor.volumes.hostPath.depdir.mount.readOnly", "true")\
    .getOrCreate()

import numpy as np

kafkaStruct = StructType([
    StructField('id', IntegerType()),
    StructField('sex', IntegerType()),                     
    StructField('bdate', StringType()),
    StructField('text', StringType())
])

splitedStruct = StructType([
    StructField("sex", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("topic", StringType(), False),
    StructField("text", StringType(), False),
    StructField("count", IntegerType(), False)
])

def define_age_group(age):
    if age < 18:
        group = '18'
    elif age < 27:
        group = '1827'
    elif age < 40:
        group = '2740'
    elif age < 60:
        group = '4060'
    else:
        group = '60'
    return group

def split(value):
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize
    from datetime import datetime

    sex = {0: 'undefined', 1: 'female', 2: 'male'}[value.sex]
    age = int((datetime.utcnow() - datetime.strptime(value.bdate, '%d.%m.%Y')).days / 365.2425)

    age_group = define_age_group(age)
    topic = sex + age_group
    words = []
    for word in word_tokenize(value.text):
        if word.lower() not in stopwords.words('russian'):
            words.append(word.lower())
    
    count = len(words)
    return (sex, age, topic, ", ".join(word for word in words), count)

df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka-svc:9092")\
    .option("subscribe", "posts")\
    .load()


df = df.select(F.from_json(col("value").cast("string"), kafkaStruct).alias("value"), 'timestamp') \
   .withColumn('value', udf(split, splitedStruct)('value')) \
   .select('value', col("value.*"), 'timestamp')

df.selectExpr('topic', 'sex', 'cast(age as string)', 'cast(text as string)', 'cast(count as string)') \
  .writeStream \
  .outputMode('update') \
  .format('console') \
  .start()

df.selectExpr('topic', 'cast(value as string)', 'cast(timestamp as string)') \
  .writeStream \
  .outputMode('update') \
  .format('kafka') \
  .option('kafka.bootstrap.servers', 'kafka-svc:9092') \
  .start()
              
for index, period in enumerate(['1 hour', '1 day', '1 week']):
    name = period.split()[1]
    df.groupBy('topic', F.window("timestamp", period, period)) \
        .sum('count').withColumnRenamed("sum(count)", f"value") \
        .withColumnRenamed("window", "timestamp") \
        .withColumn("topic", F.concat(col('topic'), F.lit("_" + name)))\
        .selectExpr("topic", "CAST(value AS STRING)", "CAST(timestamp AS STRING)") \
        .writeStream \
        .outputMode("update")\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-svc:9092") \
        .start()

spark.streams.awaitAnyTermination()