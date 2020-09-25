from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
import configparser

#!pyspark --packages=org.apache.hadoop:hadoop-aws:2.7.3

config = configparser.ConfigParser()

config.read_file(open('dwh.cfg'))

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'KEY')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'SECRET')

spark = SparkSession.builder \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.0.0') \
    .getOrCreate()

spark = None
df = spark.read.csv('s3a://udacity-labs/pagila/payment/payment.csv')
