from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

conf1=SparkConf()
sc=SparkContext(conf=conf1)
sql=SQLContext(sc)

Rdata=sc.textFile("u.data")
Mdata=sc.textFile("Movies.item")

def sepM(x):
    R=x.split("|")
    return (int(R[0]),R[1])

def sep(x):
    R=x.split("\t")
    return (int(R[1]),int(R[2]))

Rdd=Rdata.map(sep)
Rmt=Mdata.map(sepM)

schema=StructType([
    StructField('movieId',LongType(),True),
    StructField('rating',ShortType(),True)
])

Mschema=StructType([
    StructField('movieId',LongType(),True),
    StructField('title',StringType(),True)
])


b=0
def findMax(x):
    if(x>b):
       b=x
    return b

DF1=sql.createDataFrame(Rdd,schema)
DFM=sql.createDataFrame(Rmt,Mschema)


MAX=udf(findMax,FloatType())

AVGs=DF1.groupby("movieId").avg("rating")
maxAVG=AVGs.orderBy(desc("avg(rating)"))

J=DFM.join(maxAVG,DFM["movieId"]==maxAVG["movieId"]).select(DFM.movieId,"title","avg(rating)").orderBy(desc("avg(rating)"),asc("title"))
J.show()

