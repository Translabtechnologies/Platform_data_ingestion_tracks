
import os
import sys
import time
from IPython.display import display
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame , Row, functions as F, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from IPython.display import display
import pyspark

master="local" 

def init():
#{
    #Create Spark Session
    global spark
    master="local"
    appName="Sample1.py"
    spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#}

def func_code_execution():
#{
	s_df_1 = spark.read.parquet("C:/Users/DELL/Downloads/grammarframework/input/Sample1.parquet")
	s_df_1 = s_df_1.select(['Id', 'State', 'City', 'A', 'B', 'C', 'D'])
	s_df_1 = s_df_1.withColumn("col_Add",s_df_1.A + s_df_1.B)
	s_df_1 = s_df_1.withColumn("col_Sub",s_df_1.A - s_df_1.B)
	s_df_1 = s_df_1.withColumn("col_Complex",s_df_1.A+(s_df_1.B*s_df_1.A))
	s_df_1 = s_df_1.withColumn("col_Substring",substring(s_df_1['C'],2,5))
	s_df_1 = s_df_1.withColumn("col_Lpad",lpad(s_df_1['C'],10,'x'))
	s_df_1 = s_df_1.withColumn("col_Trim",trim(s_df_1['C']))
	s_df_1 = s_df_1.withColumn("col_Rtrim",rtrim(s_df_1['C']))
	s_df_1 = s_df_1.withColumn("col_Ltrim",ltrim(s_df_1['C']))
	s_df_1 = s_df_1.withColumn("D",to_timestamp(s_df_1.D,"dd-MM-yyyy HH:mm:ss"))
	s_df_1 = s_df_1.withColumn("col_DateConversion",lit(to_utc_timestamp(s_df_1['D'],'America/New_York')))
	s_df_1 = s_df_1.withColumn("col_Constantcopy",lit("maximo").cast("string"))
	t_df_1 = s_df_1
	t_df_1 = t_df_1.withColumn("col_"+"State",t_df_1["State"])
	t_df_1 = t_df_1.withColumn("col_"+"City",t_df_1["City"])
	t_df_1.show()
	t_df_1 = t_df_1.select(['Id', 'State', 'City', 'A', 'B', 'C', 'D', 'col_Add', 'col_Sub', 'col_Complex', 'col_Substring', 'col_Lpad', 'col_Trim', 'col_Rtrim', 'col_DateConversion', 'col_Constantcopy', 'col_State', 'col_City'])
	reParts = t_df_1.count() / int(1000000)
	reParts = math.ceil(reParts)
	if reParts <= 1 :
		t_df_1.write.partitionBy(['State', 'City']).mode("overwrite").parquet("C:/Users/DELL/Downloads/grammarframework/output")
	else:
		t_df_1.repartition(reParts).write.partitionBy(['State', 'City']).mode("overwrite").parquet("C:/Users/DELL/Downloads/grammarframework/output")
#}


if __name__ == '__main__':
#{
    now=datetime.now()
    current_time=now.strftime("%H:%M:%S")
    print("Start -> Start_time: {} ".format(current_time))
    init()
    func_code_execution()
    print("End -> End_time: {} ".format(current_time))
#}
