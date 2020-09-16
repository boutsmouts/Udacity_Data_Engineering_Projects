from pyspark.sql import SparkSession
from pyspark.context import SparkContext

if __name__ == '__main__':

    spark = SparkSession.builder.config('spark.ui.port', 3000).getOrCreate()

    path = 's3n://mh-udacity-dend/sparkify/sparkify_log_small.json'
    logs = spark.read.json(path)

    print(logs.head(5))

    spark.stop()
