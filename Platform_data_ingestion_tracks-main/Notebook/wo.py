
import os
import sys
import time
from IPython.display import display
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame , Row, functions as F, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
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
    appName="wo.py"
    spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
#}

def func_code_execution():
#{
	s_df_1 = spark.read.csv("C:/CS-PEROSNAL/Translab/Work/Data_Platform/Platform_data_ingestion_tracks-main/Input_DL_RZ/testmetadata.csv",header=True)
	s_df_1 = s_df_1.select( "A" ,"X" ,"B" ,"Y" ,"D" )
	s_df_2 = spark.read.parquet("C:/CS-PEROSNAL/Translab/Work/Data_Platform/Platform_data_ingestion_tracks-main/Input_DL_RZ/wo_parquet")
	s_df_1 = s_df_1.withColumn("col_Add",s_df_1.X + s_df_1.Y)
	s_df_1 = s_df_1.withColumn("col_Sub",s_df_1.X - s_df_1.Y)
	s_df_1 = s_df_1.withColumn("col_Hyb",s_df_1.X +(s_df_1.Y *s_df_1.X ))
	s_df_1 = s_df_1.withColumn("col_Substring",substring(s_df_1['B'],2,5))
	s_df_1 = s_df_1.withColumn("col_Lpad",lpad(s_df_1['A'],10,'x'))
	s_df_1 = s_df_1.withColumn("col_Trim",trim(s_df_1['A']))
	s_df_1 = s_df_1.withColumn("col_Rtrim",rtrim(s_df_1['A']))
	s_df_1 = s_df_1.withColumn("col_Ltrim",ltrim(s_df_1['A']))
	s_df_1 = s_df_1.withColumn("D",to_timestamp(s_df_1.D,"dd-MM-yyyy HH:mm:ss"))
	s_df_1 = s_df_1.withColumn("col_DateConversion",lit(to_utc_timestamp(s_df_1['D'],'America/New_York')))
	t_df_1 = s_df_1
	t_df_1 = t_df_1.select( "A" ,"X" ,"B" ,"Y" ,"D" ,"Y" ,"col_Add" ,"col_Sub" ,"col_Hyb" ,"col_Substring" ,"col_Lpad" ,"col_Trim" , "col_Rtrim" ,"col_DateConversion" )
	t_df_1.write.mode("overwrite").parquet("C:/CS-PEROSNAL/Translab/Work/Data_Platform/Platform_data_ingestion_tracks-main/Output_DL_SZ/DL_WO")
	t_df_2 = s_df_2
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
