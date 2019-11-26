from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import DataFrameWriter as W
from pyspark.sql.functions import col
from pyspark.sql.functions import concat, col, lit
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import desc
from pyspark.sql.functions import isnan, when, count, col
import timeit

spark = pyspark.sql.SparkSession.builder.appName('test').getOrCreate()
import psycopg2 as psycopg2
import pandas as pd
import numpy as np

def load_data(sql_command):
    start = timeit.default_timer()
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(
        host="192.168.1.10",
        database="rea",
        user="postgres",
        password="novotelsomerset")
    print('sql_command:')
    print(sql_command)
    data = pd.read_sql(sql_command, conn)
    print('Converting to Spark...')
    data_spark = spark.createDataFrame(data)
    print(df.printSchema())
    stop = timeit.default_timer()
    print('Time: ', stop - start) 
    return data_spark

df = load_data("SELECT longitude,latitude,property_group "
               "FROM clean.sg_transactions WHERE is_active=1")

df = df.withColumn("longitude", F.round(df["longitude"], 3)).withColumn("latitude", F.round(df["latitude"], 3))
df = df.na.drop()

# check NA in each column
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()
dist_loc=df.distinct()

dat=df.limit(1).collect()
df_type = dist_loc.filter(dist_loc.property_group==dat[0][2])
df_type=df_type.withColumn("lon",lit(dat[0][0])). withColumn("lat",lit(dat[0][1]))
#df_type.show()


df_type = df.distinct().filter(df_distinct.property_group == dat[2])
df_dist = df_type.rdd.map(get_distance).map(Row("distance")).toDF()
               
