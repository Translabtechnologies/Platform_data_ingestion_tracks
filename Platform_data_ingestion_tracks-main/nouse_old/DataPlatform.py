#!/usr/bin/env python3

import os
import sys
import time
from IPython.display import display
from datetime import datetime
'''
from pyspark.sql import SparkSession, DataFrame , Row, functions as F, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from IPython.display import display
import pyspark
'''

import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

def init():
#{
    '''
    #Create Spark Session
    appName = "DataPlatform"
    master="local"
    spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
    '''
    print("CODE STARTED")
    print("")
    print("")
#}


def readingetlconfig_DataFrame():
#{
    #Read main data
    datafeed = pd.read_excel("C:\\CS-PEROSNAL\\Translab\\Work\\Data_Platform\\DATA\\wostatus_mapping_blueprint.xlsx",sheet_name = 0)
    #display(datafeed)


    #Read meta data
    transformationDF = pd.read_excel("C:\\CS-PEROSNAL\\Translab\\Work\\Data_Platform\\DATA\\wostatus_mapping_blueprint.xlsx",sheet_name = 1)
    #display(transformationDF)

    # Read Target Schema
    targetschema = pd.read_excel("C:\\CS-PEROSNAL\\Translab\\Work\\Data_Platform\\DATA\\wostatus_mapping_blueprint.xlsx",sheet_name = 2)
    #display(targetschema)
    
    path_s = "" 
    path_t = ""
    for i in range(len(datafeed)):
        if datafeed.iloc[i,3] == "Source":
            path_s=datafeed.iloc[i,4]
            datafeedname_s=datafeed.iloc[i,1]
            v_str=datafeedname_s + '=' + 'spark.read.csv' + '(' + path_s + ',' + 'header=True' + ')'
            print(v_str)
#}


def readingetlconfig_Transformation():
#{
   
    #Read meta data
    transformationDF = pd.read_excel("C:\\CS-PEROSNAL\\Translab\\Work\\Data_Platform\\DATA\\wostatus_mapping_blueprint.xlsx",sheet_name = 1)
    v_str=""
    
    transformationDF.loc[transformationDF['Source_column'].isnull(),'T_Source_column_is_NaN'] = 'Yes'
    transformationDF.loc[transformationDF['Source_column'].notnull(), 'T_Source_column_is_NaN'] = 'No'
    
  
    
    for i in range(len(transformationDF)):
        T_SN = str(transformationDF.iloc[i,0])
        T_DATAFRAME = str(transformationDF.iloc[i,1])
        T_Source_column	= str(transformationDF.iloc[i,2])
        T_Target_column	= str(transformationDF.iloc[i,3])
        T_Tansformation	= str(transformationDF.iloc[i,4])
        T_TransformationLogic =	str(transformationDF.iloc[i,5])
        T_TransformationOrder = str(transformationDF.iloc[i,6])
        T_Source_column_is_NaN = str(transformationDF.iloc[i,7])
        
        
        
        if T_Tansformation == "FILTER":
        #{
            T_FILTER(T_DATAFRAME,T_Source_column,T_TransformationLogic)
        #}
        elif T_Tansformation == "COPY":
        #{
            T_COPY(T_DATAFRAME,T_Source_column,T_Target_column,T_Source_column_is_NaN)
        #}
        elif T_Tansformation == "SUBSTRING":
        #{
            T_SUBSTRING(T_DATAFRAME,T_Source_column,T_Target_column,T_TransformationLogic)
        #}
        elif T_Tansformation == "ADDITION":
        #{
            T_ADDITION(T_DATAFRAME,T_Target_column,T_TransformationLogic)
        #}
        else:
        #{
            print("CODE:::::::::::::::::::      else");
        #}
#}

def T_FILTER(T_DATAFRAME,T_Source_column,T_TransformationLogic):
#{
    v_str=T_DATAFRAME + '=' + T_DATAFRAME + '.filter("' + T_Source_column + T_TransformationLogic + '")'
    print(v_str)
    
#}

def T_COPY(T_DATAFRAME,T_Source_column,T_Target_column,T_Source_column_is_NaN):
#{
    if T_Source_column_is_NaN == 'Yes':
    #{
        v_str=T_DATAFRAME + '=' + T_DATAFRAME + '.withColumn("' + T_Target_column + '",' + "lit(None).cast('String'))"
    #}
    else:
    #{
        v_str=T_DATAFRAME + '=' + T_DATAFRAME + '.withColumn("' + T_Target_column + '",' + T_DATAFRAME + '.' + T_Source_column + ')'
    #}
    print(v_str)
#}

def T_SUBSTRING(T_DATAFRAME,T_Source_column,T_Target_column,T_TransformationLogic):
#{
    v_str=T_DATAFRAME + '=' + T_DATAFRAME + '.withColumn("' + T_Target_column + '",' + T_TransformationLogic + ')'
    print(v_str)
    
#}

def T_ADDITION(T_DATAFRAME,T_Target_column,T_TransformationLogic):
#{
    v_str=T_DATAFRAME + '=' + T_DATAFRAME + '.withColumn("' + T_Target_column + '",' + T_TransformationLogic + ')'
    print(v_str)
    
#}

def readingetlconfig_Target():
#{
    # Read Target Schema
    col_lst=""
    targetschema = pd.read_excel("C:\\CS-PEROSNAL\\Translab\\Work\\Data_Platform\\DATA\\wostatus_mapping_blueprint.xlsx",sheet_name = 2)
    for i in range(len(targetschema)):
        col_lst = col_lst + '"' + targetschema.iloc[i,0] + '",'
    col_lst = col_lst[:-1]
    datafeed = pd.read_excel("C:\\CS-PEROSNAL\\Translab\\Work\\Data_Platform\\DATA\\wostatus_mapping_blueprint.xlsx",sheet_name = 0)
    for i in range(len(datafeed)):
        if datafeed.iloc[i,3] == "Target":
            path_t=datafeed.iloc[i,4]
            datafeedname_s=datafeed.iloc[i,1]
            #col_lst = '"STATUS","CHANGEDATELOCAL","TARGETMEMO","FREECOL1","FREECOL2","FREECOL3","AMOUNT"'
            v_str='DF1=DF1.select(' + col_lst + ')'
            print(v_str)
            v_str='DF1.write.mode("overwrite").csv(' + path_t + ', sep = ",", header = True)'
            print(v_str)
#}

if __name__ == '__main__':
##{
    now=datetime.now()
    current_time=now.strftime("%H:%M:%S")
    print("Start -> Start_time: {} ".format(current_time))
    init()
    readingetlconfig_DataFrame()
    readingetlconfig_Transformation()
    readingetlconfig_Target()
##}


#
#PSPARK --> C:\CS\spark-3.2.1-bin-hadoop3.2\bin\spark-submit --master spark://localhost:7077 --deploy-mode client --driver-memory 12g --driver-cores 1 --executor-memory 5G --executor-cores 1 --total-executor-cores 2 C:\CS-PEROSNAL\Translab\Work\Data_Platform\CODE\DataPlatform.py > C:\CS-PEROSNAL\Translab\Work\Data_Platform\LOGS_pythoncode_py.csv
# PYTHON --> C:\CS-PEROSNAL\Translab\Work\Data_Platform\CODE\DataPlatform.py > C:\CS-PEROSNAL\Translab\Work\Data_Platform\LOGS\LOGS_pythoncode_py.txt