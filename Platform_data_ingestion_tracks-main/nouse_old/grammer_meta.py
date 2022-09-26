from pyspark.sql import SparkSession
from pyspark.sql.functions import *




#create spark session 
appName = "Grammer"
master="local"
spark = SparkSession.builder.appName(appName).master(master).getOrCreate()


#Read the grammer
df_grammer = spark.read.option("multiline","true").json("C:\\Users\\harsh\\Downloads\\Data_Platform\\Grammer\\grammer.json")
#df.printSchema()
#df.show()


#Read the sample data
df_sample_data = 'df_sample_data = spark.read.csv("C:\\\\Users\\\\harsh\\\\Downloads\\\\Data_Platform\\\\Grammer\\\\testsampledata.csv",header=True)'
print(df_sample_data)
print("df_sample_data.show()")



   
def arithmetic(exp,source,op):
    newexpr=""
    if op == 'addition':
        for i in source:
            newexpr = newexpr + "df_sample_data." + i + " + "
        return newexpr[:-3]
    elif op == 'subtraction':
        for i in source:
            newexpr = newexpr + "df_sample_data." + i + " - "
        return newexpr[:-3]
    elif op == 'multiplication':
        for i in source:
            newexpr = newexpr + "df_sample_data." + i + " * "
        return newexpr[:-3]
    elif op == 'division':
        for i in source:
            newexpr = newexpr + "df_sample_data." + i + " / "
        return newexpr[:-3]
    else:
        for i in exp:
            if i in source:
                newexpr = newexpr + "df_sample_data." + i + " "
            else:
                newexpr = newexpr + i
                
    return newexpr


def string(source,params,op):
    if op == 'lpad':
        return "lpad(df_sample_data['" +source+ "']," + str(params["max_length"]) + ",'" + params["padding_character"] + "')"
    elif op == 'substr':
        return "substring(df_sample_data['" +source+ "']," + str(params["start_position"]) + "," + str(params["no_of_character"])+ ")"            
    elif op == 'trim':
        return "trim(df_sample_data['" +source+ "'])"
    elif op == 'rtrim':
        return "rtrim(df_sample_data['" +source+ "'])"
    elif op == 'ltrim':
        return "ltrim(df_sample_data['" +source+ "'])"



if __name__ == '__main__':
    for row in df_grammer.collect():
        if row['transformation_operation'] == 'arithmetic':
            output = arithmetic(row['expression'],row['source_column'],row['transformation_operator'])
            df_sample_data = 'df_sample_data = df_sample_data.withColumn("' + row["target_column"] + '",' +output+ ')'
            print(df_sample_data)
        elif row['transformation_operation'] == 'string':
            output = string(row["source_column"][0],row["params"],row["transformation_operator"])
            df_sample_data = 'df_sample_data = df_sample_data.withColumn("' + row["target_column"] + '",lit(' + output + '))'
            print(df_sample_data)
    print("df_sample_data.show()")















#df_sample_data = df_sample_data.withColumn("target",lpad(df_sample_data.A,35,x))
#df_sample_data = df_sample_data.withColumn("target",df_sample_data.X + df_sample_data.Y).show()

#C:\BigDataLocalSetup\spark\bin\spark-submit --master spark://localhost:7077 --deploy-mode client --driver-memory 12g --driver-cores 1 --executor-memory 5G --executor-cores 1 --total-executor-cores 2 C:\Users\harsh\Downloads\Data_Platform\grammer_sample.py