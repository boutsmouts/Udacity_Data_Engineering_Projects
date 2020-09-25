from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
import pandas as pd
import matplotlib.pyplot as plt
import re


bp = PretrainedPipeline.from_disk('Explain_document_dl_en')
spark.version

dir(sparknlp.base)

#!pyspark --packages com.johnsnowlabs.nlp:spark-nlp_2.11:2.6.1
#!pip install spark-nlp

#CREATE SPARK SESSION

spark = SparkSession.builder \
    .appName("Spark NLP")\
    .master("local[4]")\
    .config("spark.driver.memory","16G")\
    .config("spark.driver.maxResultSize", "2G") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.11:2.6.1")\
    .config("spark.kryoserializer.buffer.max", "1000M")\
    .getOrCreate()

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('Spark_NLP') \
    .config('spark.jars', 'spark-nlp-assembly-2.6.1.jar') \
    .getOrCreate()

#LOAD DATA

df = spark.read.json('Data/*.json')
df.printSchema()

#EXTRACT DATA FROM DF (STRUCT)

author = 'data.author'
title = 'data.title'
date = 'data.created_utc'

dfAuthorTitle = df.select(author, title, F.to_timestamp(F.from_unixtime(date)))
dfAuthorTitle.limit(5).toPandas()

#COUNT WORDS

dfWordCount = df.select(F.explode(F.split(title, '\\s+')).alias('word')).groupBy('word').count().orderBy(F.desc('count'))
dfWordCount.limit(10).toPandas()

#USE NLP MODULE

dfAnnotated = PretrainedPipeline.annotate(dfAuthorTitle, 'title')
