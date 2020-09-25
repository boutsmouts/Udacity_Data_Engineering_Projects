from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType
import pandas as pd
import matplotlib.pyplot as plt
import re

#CREATE SPARK SESSION

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('Schema_On_Read') \
    .getOrCreate()

#LOAD DATA

dfLog = spark.read.text('NASA_access_log_Jul95.gz')

#EDA

dfLog.printSchema()
dfLog.count()
dfLog.show(5, truncate = False)

dfLog.limit(5).toPandas()

#SIMPLE PARSING /W SPLIT

dfArrays = dfLog.withColumn('tokenized', F.split('value', ' '))
dfArrays.limit(10).toPandas()

#UDF PARSING

@F.udf(MapType(StringType(), StringType()))
def parseUDF(row):

    pattern = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S+)\s*" (\d{3}) (\S+)'

    match = re.search(pattern, row)

    if match == None:
        return (line, 0)

    size_field = match.group(9)

    if size_field == '-':
        size = 0

    else:
        size = size_field

    return dict(
        host            = match.group(1),
        client_ident    = match.group(2),
        user_id         = match.group(3),
        date_time       = match.group(4),
        method          = match.group(5),
        endpoint        = match.group(6),
        protocol        = match.group(7),
        response_code   = int(match.group(8)),
        content_size    = size
    )

dfParsed = dfLog.withColumn('parsed', parseUDF('value'))
dfParsed.limit(10).toPandas()

dfParsed.printSchema()

#DATAFRAME WITH COLUMNS

fields = ['host', 'client_ident', 'user_id', 'date_time', 'method', 'endpoint', 'protocol', 'response_code', 'content_size']
exprs = ['''parsed['{}'] as {}'''.format(field, field) for field in fields]

dfClean = dfParsed.selectExpr(*exprs).withColumn('content_size', F.expr('cast(content_size as int)'))
dfClean.limit(5).toPandas()

#UDF DATETIME

@F.udf(StringType())
def dateTimeUDF(row):

    pattern_date = '[0-9]{2}/[A-Z][a-z]{2}/[0-9]{4}'
    pattern_time = '\:[0-9]{2}\:[0-9]{2}\:[0-9]{2}'

    match_date = re.search(pattern_date, row).group(0)
    match_time = re.search(pattern_time, row).group(0)

    date = match_date.replace('Jul', '07')
    time = match_time[1:]

    return date + ' ' + time


#FORMAT DATE

dfClean_Date = dfClean.withColumn('date_time', dateTimeUDF('date_time'))
dfClean_Date.limit(5).show()

dfClean_Date = dfClean_Date.withColumn('date_time', F.to_timestamp('date_time', 'dd/MM/yyyy HH:mm:ss'))
dfClean_Date.limit(5).show()
dfClean_Date.printSchema()
