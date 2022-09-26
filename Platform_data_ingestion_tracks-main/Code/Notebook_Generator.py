import json
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

def func_source(v_ctrl_Grammar_Path,v_ctrl_Grammar_Name,v_ctrl_Notebook_Name , v_ctrl_Notebook_Path):
#{
    global outF
    v_grammar_Path = v_ctrl_Grammar_Path + '\\' + v_ctrl_Grammar_Name
    with open(v_grammar_Path) as json_file:
        data = json.load(json_file)
    df_source = pd.DataFrame.from_dict(data['source'])
    for i in df_source.columns:
    #{
        executable_code=""
        if df_source[i]["sourceType"] == "csv":
        #{
            executable_code=i+' = spark.read.csv("' + df_source[i]["sourcePath"] + '",header=True)'
            #print(executable_code);
            outF.write('\t' + executable_code + '\n')
        #}
        elif df_source[i]["sourceType"] == "parquet":
        #{
            executable_code=i+' = spark.read.parquet("' + df_source[i]["sourcePath"] + '")'
            #print(executable_code);
            outF.write('\t' + executable_code + '\n')
        #}
        elif df_source[i]["sourceType"] == "MySql":
        #{
            executable_code=""
            #print(executable_code)
            outF.write('\t' + executable_code + '\n')
        #}
         
         
        
         
        if df_source[i]["sourceschema"] != "":
        #{
            executable_code = i + ' = '+ i + '.select(' + df_source[i]["sourceschema"] + ')'
            #print(executable_code)
            outF.write('\t' + executable_code + '\n')

        #}
    #}
#}
            
            
def func_transformation(v_ctrl_Grammar_Path,v_ctrl_Grammar_Name,v_ctrl_Notebook_Name , v_ctrl_Notebook_Path):
#{
    global outF
    
    def func_t_arithmetic(exp,source,op,df):
    #{
        newexpr=""
        if op == 'addition':
            for i in source:
                newexpr = newexpr + df + "."  + i + " + "
            return newexpr[:-3]
        elif op == 'subtraction':
            for i in source:
                newexpr = newexpr + df + "."  + i + " - "
            return newexpr[:-3]
        elif op == 'multiplication':
            for i in source:
                newexpr = newexpr + df + "."  + i + " * "
            return newexpr[:-3]
        elif op == 'division':
            for i in source:
                newexpr = newexpr + df +"."  + i + " / "
            return newexpr[:-3]
        else:
            for i in exp:
                if i in source:
                    newexpr = newexpr + df + "." + i + " "
                else:
                    newexpr = newexpr + i
        return newexpr
      #}


    def func_t_string(source,params,op,df):
    #{
        if op == 'lpad':
            return "lpad(" + df +  "['" +source+ "']," + str(params["max_length"]) + ",'" + params["padding_character"]  +"')"
        elif op == 'substr':
            return "substring(" + df +  "['" +source+ "']," + str(params["start_position"]) + "," + str(params["no_of_character"])+ ")"            
        elif op == 'trim':
            return "trim(" + df +  "['" +source+ "'])"
        elif op == 'rtrim':
            return "rtrim(" + df +  "['" +source+ "'])"
        elif op == 'ltrim':
            return "ltrim(" + df +  "['" +source+ "'])"
    #}


    def func_t_date(source,df,param):
    #{
        return "to_utc_timestamp(" + df +  "['" + source + "'],'America/New_York')"
    #}

    Path = v_ctrl_Grammar_Path + '\\' + v_ctrl_Grammar_Name
    
    with open(Path) as json_file:
        data = json.load(json_file)
    df_transformation = pd.DataFrame.from_dict(data['transformation'])
    
    for index, row in df_transformation.iterrows():
    #{
        v_gm_dataframe = row["dataframe"]
        v_gm_source_column_lst = row["source_column"]
        v_gm_target_column= row["target_column"]
        v_gm_transformation_operation = row["transformation_operation"]
        v_gm_transformation_operator = row["transformation_operator"]
        v_gm_expression = row["expression"]
        v_gm_param = row["params"]
        
        #print(v_gm_dataframe, v_gm_source_column_lst , v_gm_target_column , v_gm_transformation_operation , v_gm_transformation_operator , v_gm_expression , v_gm_param)
       
        if row['transformation_operation'] == 'arithmetic':
            output = func_t_arithmetic(v_gm_expression,v_gm_source_column_lst,v_gm_transformation_operator,v_gm_dataframe)
            executable_code = v_gm_dataframe + ' = '+ v_gm_dataframe + '.withColumn("' + row["target_column"] + '",' +output+ ')'
            #print(output)
            #print(executable_code)
            outF.write('\t' + executable_code + '\n')
        elif row['transformation_operation'] == 'string':
            output = func_t_string(row["source_column"][0],v_gm_param,v_gm_transformation_operator,v_gm_dataframe)
            #print(output)
            executable_code = v_gm_dataframe + ' = '+ v_gm_dataframe + '.withColumn("' + row["target_column"] + '",' + output + ')'
            #print(executable_code)
            outF.write('\t' + executable_code + '\n')
        elif row['transformation_operation'] == 'date':
            executable_code = v_gm_dataframe + ' = '+ v_gm_dataframe + '.withColumn("'  + row["source_column"][0] + '",to_timestamp(' + v_gm_dataframe + '.' + row["source_column"][0] + ',"' +  str(row["params"]["source_timeformat"]) + '"))'
            #print(executable_code)
            outF.write('\t' + executable_code + '\n')
            output = func_t_date(v_gm_source_column_lst[0],v_gm_dataframe,v_gm_param)
            executable_code = v_gm_dataframe + ' = '+ v_gm_dataframe + '.withColumn("' + row["target_column"] + '",lit(' + output + '))' 
            #print(executable_code)
            outF.write('\t' + executable_code + '\n')
    #}
#}


def func_target(v_ctrl_Grammar_Path,v_ctrl_Grammar_Name):
#{
    global outF
    v_grammar_Path = v_ctrl_Grammar_Path + '\\' + v_ctrl_Grammar_Name
    print(v_grammar_Path)
    with open(v_grammar_Path) as json_file:
        data = json.load(json_file)
    df_target = pd.DataFrame.from_dict(data['target'])
    
    
    for i in df_target.columns:
    #{
        executable_code=""
        executable_code= i+ ' = ' + df_target[i]["sourceDF"] 
        outF.write('\t' + executable_code + '\n')
        
        if df_target[i]["targetschema"] != "":
        #{
            executable_code = i + ' = '+ i + '.select(' + df_target[i]["targetschema"] + ')'
            #print(executable_code)
            outF.write('\t' + executable_code + '\n')
         #}
         
        if df_target[i]["targetType"] == "parquet":
        #{
            executable_code=i+ '.write.mode("overwrite").parquet("' + df_target[i]["targetPath"] + '")'
            outF.write('\t' + executable_code + '\n')
        #}
        elif df_target[i]["targetType"] == "csvv":
        #{
            print("csvv writing");
        #}
        elif df_target[i]["targetType"] == "MySqll":
        #{
            print("Mysqll writing");
        #}
        

#}

def func_notebook_header(v_ctrl_Notebook_Path,v_ctrl_Notebook_Name):
#{
    global outF;
    v_notbookpath_path  = v_ctrl_Notebook_Path + '\\' + v_ctrl_Notebook_Name 
    outF = open(v_notbookpath_path, "w")
    v_header_1 = '''
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
'''
    outF.write(v_header_1 + '\n')
    
    appName =  v_ctrl_Notebook_Name 
    outF.write('master="local" \n')
    
    v_header_2 = '''
def init():
#{
    #Create Spark Session
    global spark
    master="local"
    appName="''' + appName + '''"
    spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
#}
'''
    outF.write(v_header_2)
    
    v_header_3 = '''
def func_code_execution():
#{
'''
    outF.write(v_header_3)
#}

def func_notebook_footer():
#{
    global outF

    v_footer_1='''#}


if __name__ == '__main__':
#{
    now=datetime.now()
    current_time=now.strftime("%H:%M:%S")
    print("Start -> Start_time: {} ".format(current_time))
    init()
    func_code_execution()
    print("End -> End_time: {} ".format(current_time))
#}
'''

    outF.write(v_footer_1)
#}

def funcValidateGrammar():
#{
    print("funcValidateGrammar");
#}

def func_config_read(v_config_path):
#{
    global outF
    df_pd_dataflowlist=pd.read_csv(v_config_path , skiprows=0)
    for index, row in df_pd_dataflowlist.iterrows():
        v_ctrl_S_No          = row["S_No"]
        v_ctrl_Grammar_Path  = row["Grammar_Path"]
        v_ctrl_Grammar_Name  = row["Grammar_Name"]
        v_ctrl_Notebook_Name = row["Notebook_Name"]
        v_ctrl_Notebook_Path = row["Notebook_Path"]
        v_ctrl_Is_Active     = row["Is_Active"]
        print(v_ctrl_S_No , v_ctrl_Grammar_Path , v_ctrl_Grammar_Name , v_ctrl_Notebook_Name , v_ctrl_Notebook_Path , v_ctrl_Is_Active);
        
        if v_ctrl_Is_Active == 'Y':
        #{
            #funcValidateGrammar(v_ctrl_Grammar_Path,v_ctrl_Grammar_Name)
            func_notebook_header(v_ctrl_Notebook_Path,v_ctrl_Notebook_Name)
            func_source(v_ctrl_Grammar_Path,v_ctrl_Grammar_Name,v_ctrl_Notebook_Name , v_ctrl_Notebook_Path)
            func_transformation(v_ctrl_Grammar_Path,v_ctrl_Grammar_Name,v_ctrl_Notebook_Name , v_ctrl_Notebook_Path)
            func_target(v_ctrl_Grammar_Path,v_ctrl_Grammar_Name)
            func_notebook_footer()
            outF.close()
        #}
#}


if __name__ == '__main__':
##{
    v_config_path = 'C:\CS-PEROSNAL\Translab\Work\Data_Platform\Platform_data_ingestion_tracks-main\Control_pipeline_list.csv'
    func_config_read(v_config_path)
#}