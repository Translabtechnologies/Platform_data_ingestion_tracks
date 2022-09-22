from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import pandas as pd
#create spark session 
appName = "Grammer"
master="local"
spark = SparkSession.builder.appName(appName).master(master).getOrCreate()

#Read grammer.json
with open('C:\\Users\\harsh\\OneDrive\\Documents\\Data_Platform\\Grammer\\grammer.json') as json_file:
    data = json.load(json_file)

 
#Source    
df_source = pd.DataFrame.from_dict(data['source'])

#Transformation
df_transformation =spark.createDataFrame(data['transformation'])
df_transformation.show()
#df_transformation.printSchema()
#Target
df_target = pd.DataFrame.from_dict(data['target'])
#df_target.show() 


def source():
    for i in df_source.columns:
        if df_source[i]["sourceType"] == "csv":
            print(i+" = spark.read.csv(" + df_source[i]["sourcePath"] + ",header=True)")
        elif df_source[i]["sourceType"] == "parquet":
            print(i+" = spark.read.parquet(" + df_source[i]["sourcePath"] + ")")
        elif df_source[i]["sourceType"] == "Mysql":
            print("i")
            
            
def transformation():
    def arithmetic(exp,source,op,df):
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


    def string(source,params,op,df):
        #print(source,params,op,df)
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


    def date(source,df):
        return "to_utc_timestamp(" + df +  "['" + source + "'],'America/New_York')"
    
    
    for row in df_transformation.collect():
        if row['transformation_operation'] == 'arithmetic':
            output = arithmetic(row['expression'],row['source_column'],row['transformation_operator'],row['dataframe'])
            df_sample_data = row['dataframe'] + ' = '+ row['dataframe'] + '.withColumn("' + row["target_column"] + '",' +output+ ')'
            print(df_sample_data)
        elif row['transformation_operation'] == 'string':
            output = string(row["source_column"][0],row["params"],row["transformation_operator"],row['dataframe'])
            df_sample_data = row['dataframe'] + ' = '+ row['dataframe'] + '.withColumn("' + row["target_column"] + '",lit(' + output + '))'
            print(df_sample_data)
        elif row['transformation_operation'] == 'date':
            df_sample_data = row['dataframe'] + ' = '+ row['dataframe'] + '.withColumn("'  + row["source_column"][0] + '",to_timestamp(df_sample_data.' + row["source_column"][0] + ',"dd-MM-yyyy HH:mm:ss"))'
            print(df_sample_data)
            output = date(row['source_column'][0],row['dataframe'])
            df_sample_data = row['dataframe'] + ' = '+ row['dataframe'] + '.withColumn("' + row["target_column"] + '",lit(' + output + '))' 
            print(df_sample_data)
    print("df_sample_data.show()")



#def target():
 
    
    
source()    
transformation()