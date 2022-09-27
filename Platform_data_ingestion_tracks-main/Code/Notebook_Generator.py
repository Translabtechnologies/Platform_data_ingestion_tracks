import json
import pandas as pd
import re 

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
    
    def func_t_arithmetic(exp,source,op,df,target):
    #{
        output=""
        if op == 'addition':
            for i in source:
                output = output + df + "."  + i + " + "
            output = output[:-3]
            return df + ' = '+ df + '.withColumn("' + target + '",' +output+ ')'
        elif op == 'subtraction':
            for i in source:
                output = output + df + "."  + i + " - "
            output = output[:-3]
            return df + ' = '+ df + '.withColumn("' + target + '",' +output+ ')'
        elif op == 'multiplication':
            for i in source:
                output = output + df + "."  + i + " * "
            output = output[:-3]
            return df + ' = '+ df + '.withColumn("' + target + '",' +output+ ')'
        elif op == 'division':
            for i in source:
                output = output + df +"."  + i + " / "
            output = output[:-3]
            return df + ' = '+ df + '.withColumn("' + target + '",' +output+ ')'
        elif op == 'hybrid':
            output = re.sub(r"col\[" , df + "." , exp)
            output = re.sub(r"\]" , "", output)
            return df + ' = '+ df + '.withColumn("' + target + '",' +output+ ')'
        else:
            return "Throw Arithmetic Exception"
      #}


    def func_t_string(source,params,op,df,target):
    #{
        if op == 'lpad':
            output = "lpad(" + df +  "['" +source+ "']," + str(params["max_length"]) + ",'" + params["padding_character"]  +"')"
            return df + ' = '+ df + '.withColumn("' + target + '",' + output + ')'
        elif op == 'substr':
            output = "substring(" + df +  "['" +source+ "']," + str(params["start_position"]) + "," + str(params["no_of_character"])+ ")"      
            return df + ' = '+ df + '.withColumn("' + target + '",' + output + ')'
        elif op == 'trim':
            output = "trim(" + df +  "['" +source+ "'])"
            return df + ' = '+ df + '.withColumn("' + target + '",' + output + ')'
        elif op == 'rtrim':
            output = "rtrim(" + df +  "['" +source+ "'])"
            return df + ' = '+ df + '.withColumn("' + target + '",' + output + ')'
        elif op == 'ltrim':
            output = "ltrim(" + df +  "['" +source+ "'])"
            return df + ' = '+ df + '.withColumn("' + target + '",' + output + ')'
        else:
            return "Throw string exception"
    #}


    def func_t_date(source,df,params,target):
    #{
        output = params["target_timezone"] + "(" + df + "['" + source + "'],'" + params["source_timezone"] + "')"
        return df + ' = '+ df + '.withColumn("' + target + '",lit(' + output + '))'
    #}
    
    def func_t_rename(df,source,target):
    #{
        return df + ' = ' + df + '.withColumnRenamed("' + source + '","' + target +'")'
    #}
    
    def func_t_constantcopy(df,target,val,vtype,dtype):
    #{
        if vtype == 'c':
            return df + ' = ' +df + '.withColumn("' + target + '",lit("' + val + '").cast("' + dtype + '"))' 
        elif vtype == 'v':
            return "Variable constant copy"
        else:
        #{
            return "Throw copy exception"
        #}
    #}
    
    def func_t_filter(df,source,val,op,exp,ftype):
    #{
        output=""
        if ftype == '':
            return df + ' = ' + df + '.filter("' + str(source) + str(op) + str(val) + '")'
        elif ftype == 'hybrid':
            output = re.sub(r"col\[" , df + "." , exp)
            output = re.sub(r"\]" , "", output)
            return df + ' = ' + df + '.filter(' + output + ')'  
        else:
        #{
            return "Throw filter exception"
        #}
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
        v_gm_value = row["value"]
        v_gm_value_type = row["value_type"]
        v_gm_datatype = row["datatype"]
        v_gm_filter_type = row["filter_type"]
        
        #print(v_gm_dataframe, v_gm_source_column_lst , v_gm_target_column , v_gm_transformation_operation , v_gm_transformation_operator , v_gm_expression , v_gm_param)
       
        if row['transformation_operation'] == 'arithmetic':
            executable_code = func_t_arithmetic(v_gm_expression,v_gm_source_column_lst,v_gm_transformation_operator,v_gm_dataframe,v_gm_target_column)
            outF.write('\t' + executable_code + '\n')
        elif row['transformation_operation'] == 'string':
            executable_code = func_t_string(row["source_column"][0],v_gm_param,v_gm_transformation_operator,v_gm_dataframe,v_gm_target_column)
            outF.write('\t' + executable_code + '\n')
        elif row['transformation_operation'] == 'date':
            executable_code = v_gm_dataframe + ' = '+ v_gm_dataframe + '.withColumn("'  + row["source_column"][0] + '",to_timestamp(' + v_gm_dataframe + '.' + row["source_column"][0] + ',"' +  str(row["params"]["source_timeformat"]) + '"))'
            outF.write('\t' + executable_code + '\n')
            executable_code = func_t_date(v_gm_source_column_lst[0],v_gm_dataframe,v_gm_param,v_gm_target_column)
            outF.write('\t' + executable_code + '\n')
        elif row['transformation_operation'] == 'rename':
            executable_code = func_t_rename(v_gm_dataframe,v_gm_source_column_lst,v_gm_target_column)
            outF.write('\t' + executable_code + '\n')
        elif row['transformation_operation'] == 'constantcopy':
            executable_code = func_t_constantcopy(v_gm_dataframe,v_gm_target_column,v_gm_value,v_gm_value_type,v_gm_datatype)
            outF.write('\t' + executable_code + '\n')
        elif row['transformation_operation'] == 'filter':
            executable_code = func_t_filter(v_gm_dataframe,v_gm_source_column_lst,v_gm_value,v_gm_transformation_operator,v_gm_expression,v_gm_filter_type)
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
            func_notebook_header(v_ctrl_Notebook_Path,v_ctrl_Notebook_Name)
            func_source(v_ctrl_Grammar_Path,v_ctrl_Grammar_Name,v_ctrl_Notebook_Name , v_ctrl_Notebook_Path)
            func_transformation(v_ctrl_Grammar_Path,v_ctrl_Grammar_Name,v_ctrl_Notebook_Name , v_ctrl_Notebook_Path)
            outF.write('\t'+ "s_df_1.show()" + '\n')
            func_target(v_ctrl_Grammar_Path,v_ctrl_Grammar_Name)
            func_notebook_footer()
            #outF.close()
        #}
#}


if __name__ == '__main__':
##{
    v_config_path = 'C:\Platform_data_ingestion_tracks-main\Control_pipeline_list.csv'
    func_config_read(v_config_path)
#}
