from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *

conf1=SparkConf()
sc=SparkContext(conf=conf1)
sql=SQLContext(sc)

Rdd1=sc.textFile("u.data")

def split(x):
    r=x.split("\t")
    return int(r[0]),int(r[1]),int(r[2]),int(r[3])


records=Rdd1.map(split)

schema=StructType([
    StructField('userId',LongType(),True),
    StructField('movieId',LongType(),True),
    StructField('rating',ShortType(),True),
    StructField('timestamp',LongType(),True)
])

DF1=sql.createDataFrame(records,schema)

#X=DF1.groupby(DF1.movieId).sum("rating").select(func.col("sum(rating)").alias("maxDiff"))
X=DF1.groupby(DF1.movieId).sum("rating").select(DF1.movieId,"sum(rating)".alias("t"))


X.show()

