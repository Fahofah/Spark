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
# records=Rdd1.map(lambda x: x.split("\t"))
# DF1=sql.createDataFrame(records,['user_id','movie_id','rating','timestamp'])

schema=StructType([
    StructField('userId',LongType(),True),
    StructField('movieId',LongType(),True),
    StructField('rating',ShortType(),True),
    StructField('timestamp',LongType(),True)
])

DF1=sql.createDataFrame(records,schema)
# print("==DF1.chema")
# DF1.printSchema()

# print("--Df1.Show()")
# DF1.show(3)     #for all, put DF1.count() in show

# print(DF1.count())

# DF1.select("_1").show()  #column 1

# DF1.select(DF1._1).show()

# print("filter_Select")
# DF1.select(DF1._1).filter(DF1._1>100).show()

# print("advance-filter")
# X=DF1.filter((DF1._1>100)&(DF1._2<50)).select("_1")
# print(X.count())
# X.show()

# XX=DF1.filter(  (DF1._1.between(300,350))  & (DF1._3==5)    ).select(DF1._1.alias("movie"),(DF1._3*10/2).alias("Rating"))
# XX.show()

Y=DF1.filter(DF1.movieId==2).sort(DF1.rating.desc()).select(DF1.movieId,DF1.rating).show(100)