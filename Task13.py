from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf

conf1=SparkConf()
sc=SparkContext(conf=conf1)
sql=SQLContext(sc)

Rdd1=sc.textFile("regs.txt")

def sep(x):
    X=x.split("|")
    return X[0],X[1],X[2],int(X[3]),X[4]

Rdata=Rdd1.map(sep)

schema=StructType([
    StructField('regNo',StringType(),True),
    StructField('name',StringType(),True),
    StructField('class',StringType(),True),
    StructField('mark',LongType(),True),
    StructField('gender',StringType(),True)
])

DF1=sql.createDataFrame(Rdata,schema)

def grading(x):
    
    if(x>=90):
        return"A+"
    elif(x>=80 and x<90):
        return"A"
    elif(x>=70 and x<80):
        return"B"
    elif(x>=60 and x<70):
        return "C"
    elif(x<60):
        return "Fail"
    

    return G
def perc(x):
    return float(x*100/150)
        
gradeIt=udf(grading,StringType()) 
percIt=udf(perc,FloatType())

DF1.select("regNo","name","mark",percIt("mark").alias("weighted percentage"),gradeIt(percIt("mark")).alias("Grade")).show()