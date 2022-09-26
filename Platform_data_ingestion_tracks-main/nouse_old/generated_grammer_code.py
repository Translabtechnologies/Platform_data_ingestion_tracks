from pyspark.sql import SparkSession, DataFrame , Row, functions as F, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from IPython.display import display
import pyspark


#create spark session 
appName = "Grammer"
master="local"
spark = SparkSession.builder.appName(appName).master(master).getOrCreate()

df_meta_data = spark.read.csv("C:\\Users\\harsh\\Downloads\\testmetadata.csv",header=True)
df_meta_data.show()
df_meta_data = df_meta_data.withColumn("Add",df_meta_data.X + df_meta_data.Y + df_meta_data.D)
df_meta_data = df_meta_data.withColumn("Sub",df_meta_data.X - df_meta_data.Y)
df_meta_data = df_meta_data.withColumn("Mul",df_meta_data.X * df_meta_data.Y * df_meta_data.D)
df_meta_data = df_meta_data.withColumn("Div",df_meta_data.X / df_meta_data.Y)
df_meta_data = df_meta_data.withColumn("Hyb",df_meta_data.X +(df_meta_data.Y *df_meta_data.X ))
df_meta_data = df_meta_data.withColumn("Lpad",lit(lpad(df_meta_data['A'],10,'x')))
df_meta_data = df_meta_data.withColumn("Substring",lit(substring(df_meta_data['B'],2,5)))
df_meta_data = df_meta_data.withColumn("Trim",lit(trim(df_meta_data['C'])))
df_meta_data = df_meta_data.withColumn("Rtrim",lit(rtrim(df_meta_data['C'])))
df_meta_data = df_meta_data.withColumn("Ltrim",lit(ltrim(df_meta_data['C'])))
df_meta_data.show()