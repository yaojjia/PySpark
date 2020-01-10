import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import os 

# sparkClassPath = os.getenv('SPARK_CLASSPATH','D:/spark/spark-3.0.0-preview-bin-hadoop2.7/jars/*')
spark = SparkSession \
    .builder \
    .appName("REA")\    
    .config("spark.driver.extraClassPath", sparkClassPath) \
    .getOrCreate()
   

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://192.168.1.10/rea") \
    .option("dbtable", "(SELECT latitude, longitude FROM singapore.transactions) AS tmp") \
    .option("user", "postgres") \
    .option("password", "novotelsomerset") \
    .option("driver", "org.postgresql.Driver") \
    .load()


df.printSchema()
df.show(5)
