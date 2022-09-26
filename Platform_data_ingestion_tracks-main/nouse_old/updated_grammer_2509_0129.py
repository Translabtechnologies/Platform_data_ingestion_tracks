#from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
import json
import pandas as pd
#create spark session 
#appName = "Grammer"
#master="local"
#spark = SparkSession.builder.appName(appName).master(master).getOrCreate()

#Read grammer.json
with open('C:\\CS-PEROSNAL\\Translab\\Work\\Data_Platform\\Platform_data_ingestion_tracks-main\\wo_grammar.json') as json_file:
    data = json.load(json_file)

print(data);

 
#Source    
df_source = pd.DataFrame.from_dict(data['source'])
#Transformation
#df_transformation = spark.createDataFrame(data['transformation'])
#df_transformation.show(truncate=False)
#df_transformation.printSchema()
#Target
df_target = pd.DataFrame.from_dict(data['target'])
#df_target.show() 

df_transformation = pd.DataFrame.from_dict(data['transformation'])


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

def source():
#{
    for i in df_source.columns:
        if df_source[i]["sourceType"] == "csv":
            print(i+' = spark.read.csv("' + df_source[i]["sourcePath"] + '",header=True)')
        elif df_source[i]["sourceType"] == "parquet":
            print(i+" = spark.read.parquet(" + df_source[i]["sourcePath"] + ")")
        elif df_source[i]["sourceType"] == "Mysql":
            print("Mysql")
#}
            
            
def transformation():
#{
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
            '''
            print("inside lpad");
            print("source   - " , source);
            print("params max_length   - " , params["max_length"]);
            print("params padding_character   - " , params["max_length"]);

            print("op   - " , source);
            print("df   - " , df);
            print("lpad(" + df +  "['" +source+ "']," + str(params["max_length"]) + ",'" + params["padding_character"]  +"')");
            #exit(0)
            '''
            return "lpad(" + df +  "['" +source+ "']," + str(params["max_length"]) + ",'" + params["padding_character"]  +"')"
        elif op == 'substr':
            return "substring(" + df +  "['" +source+ "']," + str(params["start_position"]) + "," + str(params["no_of_character"])+ ")"            
        elif op == 'trim':
            return "trim(" + df +  "['" +source+ "'])"
        elif op == 'rtrim':
            return "rtrim(" + df +  "['" +source+ "'])"
        elif op == 'ltrim':
            return "ltrim(" + df +  "['" +source+ "'])"


    def date(source,df,param):
        return "to_utc_timestamp(" + df +  "['" + source + "'],'America/New_York')"
    
    '''
    print("97")
    print(df_transformation);
   
    #df = pd.read_json("C:\\CS-PEROSNAL\\Translab\\Work\\Data_Platform\\Platform_data_ingestion_tracks-main\\grammer.json")
    #df.head()
    for index, row in df_transformation.iterrows():
        print(row['transformation_operation'])
        print(row['params'])
        
        print(str(row['params']["start_position"]))
        #print(row['params']['padding_character'].str())
        #print(row['params']['start_position'].str())
        #print(row['params']['no_of_character'].str())
    exit(0)
    
    for cur_row in df_transformation:
        print(cur_row);
        #print(df_transformation[i]["transformation_operation"]);
        #print(df_transformation[i])
        #print(df_transformation[i])
        #print(df[i]["transformation_operation"])
    exit(0)    
    
    '''

    

    
    #for row in df_transformation:
    for index, row in df_transformation.iterrows():
        v_gm_param = row["params"]
        if row['transformation_operation'] == 'arithmetic':
            output = arithmetic(row['expression'],row['source_column'],row['transformation_operator'],row['dataframe'])
            df_sample_data = row['dataframe'] + ' = '+ row['dataframe'] + '.withColumn("' + row["target_column"] + '",' +output+ ')'
            #print(output)
            print(df_sample_data)
        elif row['transformation_operation'] == 'string':
            output = string(row["source_column"][0],row["params"],row["transformation_operator"],row['dataframe'])
            #print(output)
            df_sample_data = row['dataframe'] + ' = '+ row['dataframe'] + '.withColumn("' + row["target_column"] + '",' + output + ')'
            print(df_sample_data)
        elif row['transformation_operation'] == 'date':
            df_sample_data = row['dataframe'] + ' = '+ row['dataframe'] + '.withColumn("'  + row["source_column"][0] + '",to_timestamp(' + row['dataframe'] + '.' + row["source_column"][0] + ',"' +  str(row["params"]["source_timeformat"]) + '"))'
            print(df_sample_data)
            output = date(row['source_column'][0],row['dataframe'],row["params"])
            df_sample_data = row['dataframe'] + ' = '+ row['dataframe'] + '.withColumn("' + row["target_column"] + '",lit(' + output + '))' 
            print(df_sample_data)
    #print("df_sample_data.show()")

#}

#def target():
 
    
    
source()    
transformation()